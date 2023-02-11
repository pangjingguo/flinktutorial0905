package com.atguigu.day05;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example4 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
                        // 新建一个水位线发生器
                        new WatermarkStrategy<Tuple2<String, Long>>() {
                            @Override
                            public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                                return new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1; // 告诉flink哪个字段是事件时间戳
                                    }
                                };
                            }

                            @Override
                            public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Tuple2<String, Long>>() {
                                    private final long delay = 5000L; // 最大延迟时间5秒钟
                                    // 为了防止溢出，加delay+1
                                    private long maxTimestamp = Long.MIN_VALUE + delay + 1L; // 保存观察到的最大事件时间戳
                                    @Override
                                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                                        // onEvent每来一条数据触发一次
                                        // 每来一条数据，尝试更新一次观察到的最大事件时间戳
                                        maxTimestamp = Math.max(event.f1, maxTimestamp);

                                        // 针对特殊数据发送水位线
                                        if (event.f0.equals("Mary")) {
                                            output.emitWatermark(new Watermark(Long.MAX_VALUE));
                                        }
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        // onPeriodicEmit周期性触发，默认每隔200毫秒触发一次
                                        // 默认公式
                                        output.emitWatermark(new Watermark(maxTimestamp - delay - 1L));
                                    }
                                };
                            }
                        }
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
