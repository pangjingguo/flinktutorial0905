package com.atguigu.day02;

import com.atguigu.util.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example2 {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource()).print();

        env.execute();
    }
}
