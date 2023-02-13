package com.atguigu.day06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example1 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var stream1 = env
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
                );

        var stream2 = env
                // "a 1" 事件时间是1s
                // "a 2" 事件时间是2s
                .socketTextStream("localhost", 9998)
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
                );

        stream1
                .union(stream2)
                .process(new ProcessFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> in, Context ctx, Collector<String> out) throws Exception {
                        out.collect("输入数据：" + in + ", 当前process并行子任务的水位线：" + ctx.timerService().currentWatermark());
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
