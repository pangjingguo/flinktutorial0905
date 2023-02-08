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

public class Example10 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntegerSource())
                .keyBy(r -> "integer")
                .process(new KeyedProcessFunction<String, Integer, IntegerStatistic>() {
                    // 声明一个状态变量，用来保存统计信息
                    private ValueState<IntegerStatistic> accumulator;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        accumulator = getRuntimeContext().getState(
                                new ValueStateDescriptor<IntegerStatistic>(
                                        "acc", // 值状态变量的名字
                                        Types.POJO(IntegerStatistic.class) // 值状态变量中保存的数据类型
                                )
                        );
                    }

                    @Override
                    public void processElement(Integer in, Context ctx, Collector<IntegerStatistic> out) throws Exception {
                        // accumulator.value()取出的是输入数据in的key所对应的值状态变量
                        if (accumulator.value() == null) {
                            // 第一条数据到来
                            accumulator.update(new IntegerStatistic(
                                    in, in, 1, in, in
                            ));
                        } else {
                            var oldAccumulator = accumulator.value();
                            var newAccumulator = new IntegerStatistic(
                                    Math.max(oldAccumulator.max, in),
                                    Math.min(oldAccumulator.min, in),
                                    oldAccumulator.count + 1,
                                    oldAccumulator.sum + in,
                                    (oldAccumulator.sum + in) / (oldAccumulator.count + 1)
                            );
                            accumulator.update(newAccumulator);
                        }

                        out.collect(accumulator.value());
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
