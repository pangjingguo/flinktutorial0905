package com.atguigu.day02;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example10 {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4)
                .setParallelism(1)
                .shuffle()
                .print("随机发送")
                .setParallelism(2);

        env
                .fromElements(1,2,3,4)
                .setParallelism(1)
                .rebalance()
                .print("rebalance")
                .setParallelism(4);

        env
                .fromElements(1)
                .setParallelism(1)
                .broadcast()
                .print("广播")
                .setParallelism(4);

        env
                .fromElements(1,2,3,4)
                .setParallelism(1)
                .global()
                .print("global")
                .setParallelism(10);

        env.execute();
    }
}
