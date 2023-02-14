package com.atguigu.day07;

import com.atguigu.util.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example4 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var leftStream = env
                .fromElements(
                        new Event("order-1", "left", 10 * 1000L),
                        new Event("order-1", "left", 11 * 1000L),
                        new Event("order-1", "left", 12 * 1000L),
                        new Event("order-1", "left", 20 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.ts;
                            }
                        })
                );

        var rightStream = env
                .fromElements(
                        new Event("order-1", "right", 6 * 1000L),
                        new Event("order-1", "right", 7 * 1000L),
                        new Event("order-1", "right", 12 * 1000L),
                        new Event("order-1", "right", 24 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.ts;
                                    }
                                })
                );

        leftStream.keyBy(r -> r.key)
                .intervalJoin(rightStream.keyBy(r -> r.key))
                .between(Time.seconds(-5), Time.seconds(6))
                .process(new ProcessJoinFunction<Event, Event, String>() {
                    @Override
                    public void processElement(Event left, Event right, Context context, Collector<String> collector) throws Exception {
                        collector.collect(left + " ==> " + right);
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
