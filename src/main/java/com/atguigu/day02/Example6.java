package com.atguigu.day02;

import com.atguigu.util.IntegerSource;
import com.atguigu.util.IntegerStatistic;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example6 {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntegerSource())
                .map(r -> new IntegerStatistic(r, r, 1, r, r))
                .keyBy(r -> "integer")
                .reduce(new ReduceFunction<IntegerStatistic>() {
                    @Override
                    public IntegerStatistic reduce(IntegerStatistic accumulator, IntegerStatistic in) throws Exception {
                        return new IntegerStatistic(
                                Math.max(accumulator.max, in.max),
                                Math.min(accumulator.min, in.min),
                                accumulator.count + in.count,
                                accumulator.sum + in.sum,
                                (accumulator.sum + in.sum) / (accumulator.count + in.count)
                        );
                    }
                })
                .print();

        env.execute();
    }
}
