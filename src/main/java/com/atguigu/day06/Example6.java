package com.atguigu.day06;

import com.atguigu.util.ProductViewCountPerWindow;
import com.atguigu.util.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class Example6 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flinktutorial0905/src/main/resources/UserBehavior.csv")
                .setParallelism(1)
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String in, Collector<UserBehavior> out) throws Exception {
                        var fields = in.split(",");
                        var userBehavior = new UserBehavior();
                        userBehavior.productId = fields[1];
                        userBehavior.type = fields[3];
                        userBehavior.ts = Long.parseLong(fields[4]) * 1000L;

                        if (userBehavior.type.equals("pv"))
                            out.collect(userBehavior);
                    }
                })
                .setParallelism(1)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.ts;
                            }
                        })
                )
                .setParallelism(1)
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
                            public void process(String productId, Context context, Iterable<Long> elements, Collector<ProductViewCountPerWindow> out) throws Exception {
                                out.collect(new ProductViewCountPerWindow(
                                        productId,
                                        elements.iterator().next(),
                                        context.window().getStart(),
                                        context.window().getEnd()
                                ));
                            }
                        }
                )
                .setParallelism(4)
                // ?????????????????????????????????????????????
                .keyBy(new KeySelector<ProductViewCountPerWindow, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(ProductViewCountPerWindow value) throws Exception {
                        return Tuple2.of(value.windowStartTime, value.windowEndTime);
                    }
                })
                .process(new TopN(3))
                .setParallelism(2)
                .print()
                .setParallelism(2);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class TopN extends KeyedProcessFunction<Tuple2<Long, Long>, ProductViewCountPerWindow, String> {
        // ??????n???
        private final int n;

        public TopN(int n) {
            this.n = n;
        }

        // ?????????????????????????????????????????????????????????????????????????????????
        private ListState<ProductViewCountPerWindow> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<>(
                            "list-state",
                            Types.POJO(ProductViewCountPerWindow.class)
                    )
            );
        }

        @Override
        public void processElement(ProductViewCountPerWindow in, Context ctx, Collector<String> out) throws Exception {
            listState.add(in);

            // ???????????????????????????????????????????????????????????????
            ctx.timerService().registerEventTimeTimer(in.windowEndTime + 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            var arrayList = new ArrayList<ProductViewCountPerWindow>();
            for (var i : listState.get()) arrayList.add(i);
            // listState????????????????????????????????????????????????
            listState.clear();

            // ????????????
            arrayList.sort(new Comparator<ProductViewCountPerWindow>() {
                @Override
                public int compare(ProductViewCountPerWindow p1, ProductViewCountPerWindow p2) {
                    return (int)(p2.count - p1.count);
                }
            });

            // ????????????
            var windowStartTime = new Timestamp(ctx.getCurrentKey().f0).toString();
            var windowEndTime = new Timestamp(ctx.getCurrentKey().f1).toString();

            // ???????????????
            var result = new StringBuilder();
            result.append("=========================").append(windowStartTime).append("~").append(windowEndTime).append("=========================\n");

            for (int i = 0; i < n; i++) {
                var tmp = arrayList.get(i);
                result.append("???").append(i + 1).append("????????????id???: ").append(tmp.productId).append(", ???????????????: ").append(tmp.count).append("\n");
            }

            result.append("=========================").append(windowStartTime).append("~").append(windowEndTime).append("=========================\n");

            out.collect(result.toString());
        }
    }
}