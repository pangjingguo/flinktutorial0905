package com.atguigu.day06;

import com.atguigu.util.ProductViewCountPerWindow;
import com.atguigu.util.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class Example7 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flinktutorial0905/src/main/resources/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
                        var fields = value.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                fields[1],
                                fields[3],
                                Long.parseLong(fields[4]) * 1000L
                        );

                        if (userBehavior.type.equals("pv"))
                            out.collect(userBehavior);
                    }
                })
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
                .process(new MySlidingEventTimeWindow(60 * 60 * 1000L, 5 * 60 * 1000L))
                .keyBy(r -> r.windowEndTime)
                .process(new TopN(3))
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MySlidingEventTimeWindow extends KeyedProcessFunction<String, UserBehavior, ProductViewCountPerWindow> {
        private final long windowSize;
        private final long windowStep;

        public MySlidingEventTimeWindow(long windowSize, long windowStep) {
            this.windowSize = windowSize;
            this.windowStep = windowStep;
        }

        // MapState<窗口信息，窗口的累加器>
        private MapState<Tuple2<Long, Long>, Long> mapState;
        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Tuple2<Long, Long>, Long>(
                            "windowInfo-accumulator",
                            Types.TUPLE(Types.LONG, Types.LONG),
                            Types.LONG
                    )
            );
        }

        @Override
        public void processElement(UserBehavior in, Context ctx, Collector<ProductViewCountPerWindow> out) throws Exception {
            // 保存输入数据in属于的所有窗口的数组
            var windowInfos = new ArrayList<Tuple2<Long, Long>>();

            // in属于的第一个窗口的开始时间
            var firstWindowStartTime = in.ts - in.ts % windowStep;
            // 计算in属于的所有窗口信息
            for (long start = firstWindowStartTime; start > in.ts - windowSize; start = start - windowStep) {
                windowInfos.add(Tuple2.of(start, start + windowSize));
            }

            // 遍历窗口信息
            for (var windowInfo : windowInfos) {
                if (!mapState.contains(windowInfo)) {
                    mapState.put(windowInfo, 1L);
                } else {
                    mapState.put(windowInfo, mapState.get(windowInfo) + 1L);
                }

                // 在窗口结束时间-1毫秒的地方注册定时器
                ctx.timerService().registerEventTimeTimer(windowInfo.f1 - 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ProductViewCountPerWindow> out) throws Exception {
            long windowEndTime = timestamp + 1L;
            long windowStartTime = windowEndTime - windowSize;
            long count = mapState.get(Tuple2.of(windowStartTime, windowEndTime));
            String productId = ctx.getCurrentKey();

            out.collect(new ProductViewCountPerWindow(productId, count, windowStartTime, windowEndTime));

            mapState.remove(Tuple2.of(windowStartTime, windowEndTime));
        }
    }

    public static class TopN extends KeyedProcessFunction<Long, ProductViewCountPerWindow, String> {
        private final int n;

        public TopN(int n) {
            this.n = n;
        }

        private ListState<ProductViewCountPerWindow> listState;
        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ProductViewCountPerWindow>(
                            "list",
                            Types.POJO(ProductViewCountPerWindow.class)
                    )
            );
        }

        @Override
        public void processElement(ProductViewCountPerWindow value, Context ctx, Collector<String> out) throws Exception {
            listState.add(value);

            ctx.timerService().registerEventTimeTimer(value.windowEndTime + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            var arrayList = new ArrayList<ProductViewCountPerWindow>();
            for (var i : listState.get()) arrayList.add(i);
            listState.clear();

            arrayList.sort(new Comparator<ProductViewCountPerWindow>() {
                @Override
                public int compare(ProductViewCountPerWindow p1, ProductViewCountPerWindow p2) {
                    return (int)(p2.count - p1.count);
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("=============").append(new Timestamp(timestamp - 1L)).append("==================\n");
            for (int i = 0; i < n; i++) {
                var tmp = arrayList.get(i);
                result.append("No.").append(i+1).append(":").append(tmp.productId).append(",count=").append(tmp.count).append("\n");
            }
            result.append("=============").append(new Timestamp(timestamp - 1L)).append("==================\n");
            out.collect(result.toString());
        }
    }
}
