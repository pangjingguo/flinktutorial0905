package com.atguigu.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example8 {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .keyBy(r -> r % 3)
                .reduce(Integer::sum)
                .setParallelism(4)
                .print()
                .setParallelism(4);

        env.execute();
    }
}
