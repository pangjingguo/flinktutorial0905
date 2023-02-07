package com.atguigu.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example11 {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .rebalance()
                .map(r -> r)
                .setParallelism(2)
                .rescale()
                .print()
                .setParallelism(4);

        env.execute();
    }
}
