package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example2 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements(1,2,3,4,5)
                .keyBy(r -> r % 2)
                .reduce(Integer::sum)
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
