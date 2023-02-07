package com.atguigu.day02;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example12 {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4,5,6,7,8)
                .setParallelism(1)
                .partitionCustom(
                        // 指定某个key的数据要去哪个下游的并行子任务
                        new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int i) {
                                if (key == 0) {
                                    return 0; // 将key为0的数据发送到索引为0的并行子任务
                                } else if (key == 1) {
                                    return 1;
                                } else {
                                    return 2;
                                }
                            }
                        },
                        // 指定输入数据的key
                        r -> r % 3
                )
                .print()
                .setParallelism(4);

        env.execute();
    }
}
