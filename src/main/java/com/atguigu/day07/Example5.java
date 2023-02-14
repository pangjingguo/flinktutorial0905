package com.atguigu.day07;

import com.atguigu.util.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example5 {
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

        leftStream
                .join(rightStream)
                .where(left -> left.key)
                .equalTo(right -> right.key)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Event, Event, String>() {
                    @Override
                    public String join(Event left, Event right) throws Exception {
                        return left + " ===> " + right;
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
