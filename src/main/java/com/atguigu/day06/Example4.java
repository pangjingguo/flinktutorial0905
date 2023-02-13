package com.atguigu.day06;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Example4 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<String> result = env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        ctx.collectWithTimestamp("a", 1000L);
                        ctx.collectWithTimestamp("a", 2000L);
                        ctx.emitWatermark(new Watermark(2000L));
                        ctx.collectWithTimestamp("a", 1500L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        if (ctx.timestamp() < ctx.timerService().currentWatermark()) {
                            ctx.output(
                                    // 侧输出流的名字，单例
                                    new OutputTag<String>("late-event"){},
                                    // 发送的数据类型
                                    "输入数据" + in + "迟到了"
                            );
                        } else {
                            out.collect(in);
                        }
                    }
                });

        result.print("main");

        result.getSideOutput(new OutputTag<String>("late-event"){}).print("side");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
