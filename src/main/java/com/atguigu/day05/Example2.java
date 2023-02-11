package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example2 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 设置assignTimestampsAndWatermarks每隔1分钟向下游发送一条水位线事件
        env.getConfig().setAutoWatermarkInterval(60 * 1000L);

        env
                // "a 1" 事件时间是1s
                // "a 2" 事件时间是2s
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String s) throws Exception {
                        return Tuple2.of(
                                s.split(" ")[0],
                                // 将时间戳转换为毫秒单位
                                Long.parseLong(s.split(" ")[1]) * 1000L
                        );
                    }
                })
                // 在map输出的数据流中插入水位线事件，默认每隔200毫秒插入一次
                .assignTimestampsAndWatermarks(
                        // 配置最大延迟时间Duration.ofSeconds(5)
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                // 告诉flink元组中的哪一个字段是事件时间
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long l) {
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(r -> r.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    // elements.spliterator().getExactSizeIfKnown()返回迭代器中的元素数量
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("窗口：" + context.window().getStart() + "~" + context.window().getEnd() + "有" +
                                "" + elements.spliterator().getExactSizeIfKnown() + "条数据");
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
