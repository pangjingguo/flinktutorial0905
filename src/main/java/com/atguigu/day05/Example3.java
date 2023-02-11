package com.atguigu.day05;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example3 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new SourceFunction<String>() {
                    @Override
                    public void run(SourceContext<String> ctx) throws Exception {
                        // 如何为发送的数据指定事件时间戳呢？
                        ctx.collectWithTimestamp("hello world", 5000L);
                        ctx.emitWatermark(new Watermark(1000 * 1000L));

                        // 整条数据流中有几个事件？
                        // Watermark(-MAX)
                        // ("hello world", 5000L)
                        // Watermark(1000秒)
                        // Watermark(MAX)
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> r)
                .process(new KeyedProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String in, Context ctx, Collector<String> out) throws Exception {
                        // ctx.timestamp()获取输入数据的事件时间
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 100 * 1000L);
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 200 * 1000L);

                        out.collect("输入数据是：" + in + ", 输入数据的key是：" + ctx.getCurrentKey() + ", " +
                                "输入数据的事件时间是：" + ctx.timestamp() + ", 当前process算子的水位线是：" +
                                "" + ctx.timerService().currentWatermark() + ", " +
                                "注册的定时器的时间戳是：" + new Timestamp(ctx.timestamp() + 100 * 1000L) + "," +
                                "注册的定时器的时间戳是：" + new Timestamp(ctx.timestamp() + 200 * 1000L));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器的时间戳是：" + new Timestamp(timestamp) + ", 定时器的key是：" +
                                "" + ctx.getCurrentKey() + "，当前process算子的水位线是：" + ctx.timerService().currentWatermark());
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
