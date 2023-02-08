package com.atguigu.day03;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example8 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .keyBy(r -> "key")
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        // 当前的处理时间(机器时间)
                        long currentTimestamp = ctx.timerService().currentProcessingTime();
                        // 30s之后的时间戳
                        long thirtySecondsLater = currentTimestamp + 30 * 1000L;
                        // 60s之后的时间戳
                        long sixtySecondsLater = currentTimestamp + 60 * 1000L;

                        // 注册定时器
                        // 注册的是输入数据的key所对应的定时器
                        ctx.timerService().registerProcessingTimeTimer(thirtySecondsLater);
                        ctx.timerService().registerProcessingTimeTimer(sixtySecondsLater);

                        out.collect("输入数据：" + in + ", key为：" + ctx.getCurrentKey() + "数据到达的时间是：" +
                                "" + new Timestamp(currentTimestamp) + ", 注册了两个定时器：" +
                                "" + new Timestamp(thirtySecondsLater) + ", " + new Timestamp(sixtySecondsLater));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // timestamp参数是注册的定时器的时间戳
                        out.collect("key为" + ctx.getCurrentKey() + "的定时器触发了，定时器的注册时间戳是：" +
                                "" + new Timestamp(timestamp) + ", 定时器触发的真正时间是：" +
                                "" + new Timestamp(ctx.timerService().currentProcessingTime()));
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
