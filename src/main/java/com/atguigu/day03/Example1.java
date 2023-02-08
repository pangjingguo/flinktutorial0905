package com.atguigu.day03;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class Example1 {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .addSink(new SinkFunction<ClickEvent>() {
                    // 每来一条数据，触发一次调用
                    @Override
                    public void invoke(ClickEvent input, Context context) throws Exception {
                        System.out.println(input);
                    }
                });

        env.execute();
    }
}
