package com.atguigu.day08;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class Example3 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(10 * 1000L);

        env.addSource(new CounterSource()).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class CounterSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {
        private boolean running = true;
        private long offset = 0L; // 偏移量的初始值
        @Override
        public void run(SourceContext<Long> sourceContext) throws Exception {
            // 获取一把锁
            final Object lock = sourceContext.getCheckpointLock();
            while (running) {
                synchronized (lock) {
                    sourceContext.collect(offset);
                    offset += 1L;
                }

                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        // 声明一个算子状态变量，用来保存刚消费完的偏移量
        // 算子状态不能使用ValueState
        // 所以被迫无奈，只好使用ListState，里面只保存一条数据，就是刚消费完的偏移量
        private ListState<Long> state;

        // 程序启动时触发调用
        // 程序故障恢复时触发调用
        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            // 初始化状态变量
            state = ctx.getOperatorStateStore().getListState(
                    new ListStateDescriptor<Long>(
                            "offset",
                            Types.LONG
                    )
            );

            for (var l : state.get()) {
                offset = l;
            }
        }

        // 对状态变量作快照时触发调用
        // 当source算子的并行子任务接收到作业管理器注入的检查点分界线的时候作快照
        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            state.clear();
            state.add(offset);
        }
    }
}
