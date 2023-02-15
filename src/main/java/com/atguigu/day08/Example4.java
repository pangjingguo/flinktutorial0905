package com.atguigu.day08;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Example4 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(10 * 1000L);

        env
                .addSource(new SourceFunction<Long>() {
                    private boolean running = true;
                    private long count = 1L;
                    @Override
                    public void run(SourceContext<Long> ctx) throws Exception {
                        while (running) {
                            ctx.collect(count);
                            count++;
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .addSink(new TransactionalFileSink());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // TwoPhaseCommitSinkFunction<输入的泛型, 事务的泛型, 上下文的泛型>
    public static class TransactionalFileSink extends TwoPhaseCommitSinkFunction<Long, String, Void> {
        // 声明一个缓冲区
        private BufferedWriter bufferedWriter;

        public TransactionalFileSink() {
            super(StringSerializer.INSTANCE, VoidSerializer.INSTANCE);
        }

        // 数据流中的第一条数据到来时，开启事务
        // 每次检查点完成后，开启事务
        @Override
        protected String beginTransaction() throws Exception {
            // 创建临时文件
            long timeNow = System.currentTimeMillis();
            int taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            // 创建事务的名字
            String transactionName = timeNow + "-" + taskIdx;
            // 获取临时文件的路径
            var tempFilePath = Paths.get("/home/zuoyuan/flinktutorial0905/src/main/resources/tempfiles/" + transactionName);
            // 创建临时文件
            Files.createFile(tempFilePath);
            // 将缓冲区设置为临时文件的缓冲区
            bufferedWriter = Files.newBufferedWriter(tempFilePath);
            // 返回事务名字
            return transactionName;
        }

        // 每来一条数据调用一次
        // 将数据写入到缓冲区
        @Override
        protected void invoke(String transactionName, Long in, Context context) throws Exception {
            bufferedWriter.write(in + "\n");
        }

        // 预提交时触发执行，将缓冲区中的数据写入临时文件
        @Override
        protected void preCommit(String transactionName) throws Exception {
            bufferedWriter.flush(); // 刷到磁盘上
            bufferedWriter.close();
        }

        // sink算子收到作业管理器发来的通知时，触发执行
        // 将临时文件改名为正式文件名
        @Override
        protected void commit(String transactionName) {
            var tempFilePath = Paths.get("/home/zuoyuan/flinktutorial0905/src/main/resources/tempfiles/" + transactionName);
            if (Files.exists(tempFilePath)) {
                try {
                    // 创建正式文件名
                    var commitFilePath = Paths.get("/home/zuoyuan/flinktutorial0905/src/main/resources/targetfiles/" + transactionName);
                    Files.move(tempFilePath, commitFilePath); // 改名
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // 发生故障时触发执行
        // 删除临时文件
        @Override
        protected void abort(String transactionName) {
            var tempFilePath = Paths.get("/home/zuoyuan/flinktutorial0905/src/main/resources/tempfiles/" + transactionName);
            if (Files.exists(tempFilePath)) {
                try {
                    Files.delete(tempFilePath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
