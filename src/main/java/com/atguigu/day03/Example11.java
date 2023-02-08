package com.atguigu.day03;

import com.atguigu.util.IntegerSource;
import com.atguigu.util.IntegerStatistic;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Example11 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntegerSource())
                .keyBy(r -> "integer")
                .process(new KeyedProcessFunction<String, Integer, IntegerStatistic>() {
                    private ValueState<IntegerStatistic> acc;
                    // 标志位: 当前是否存在发送统计结果的定时器
                    private ValueState<Boolean> flag;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        acc = getRuntimeContext().getState(
                                new ValueStateDescriptor<>(
                                        "accumulator",
                                        Types.POJO(IntegerStatistic.class)
                                )
                        );
                        flag = getRuntimeContext().getState(
                                new ValueStateDescriptor<>(
                                        "flag",
                                        Types.BOOLEAN
                                )
                        );
                    }

                    @Override
                    public void processElement(Integer in, Context ctx, Collector<IntegerStatistic> out) throws Exception {
                        if (acc.value() == null) {
                            acc.update(new IntegerStatistic(in, in, 1, in, in));
                        } else {
                            var oldAcc = acc.value();
                            var newAcc = new IntegerStatistic(
                                    Math.max(oldAcc.max, in),
                                    Math.min(oldAcc.min, in),
                                    oldAcc.count + 1,
                                    oldAcc.sum + in,
                                    (oldAcc.sum + in) / (oldAcc.count + 1)
                            );
                            acc.update(newAcc);
                        }

                        // 判断是否存在定时器
                        if (flag.value() == null) {
                            ctx.timerService().registerProcessingTimeTimer(
                                    ctx.timerService().currentProcessingTime() + 10 * 1000L
                            );
                            flag.update(true);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<IntegerStatistic> out) throws Exception {
                        out.collect(acc.value());
                        flag.clear();
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
