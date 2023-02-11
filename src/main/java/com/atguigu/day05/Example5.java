package com.atguigu.day05;

import com.atguigu.util.ProductViewCountPerWindow;
import com.atguigu.util.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Example5 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 实例化一个数据源
        var source = FileSource
                .forRecordStreamFormat(
                        // 按行消费
                        new TextLineInputFormat(),
                        new Path("/home/zuoyuan/flinktutorial0905/src/main/resources/UserBehavior.csv")
                )
                .build();

        env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "userbehavior")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String in) throws Exception {
                        var fields = in.split(",");
                        return new UserBehavior(fields[1], fields[3], Long.parseLong(fields[4]) * 1000L);
                    }
                })
                .filter(r -> r.type.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .keyBy(r -> r.productId)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)))
                .aggregate(
                        new AggregateFunction<UserBehavior, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(UserBehavior value, Long accumulator) {
                                return accumulator + 1L;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Long, ProductViewCountPerWindow, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
                                out.collect(new ProductViewCountPerWindow(key, elements.iterator().next(), context.window().getStart(), context.window().getEnd()));
                            }
                        }
                )
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
