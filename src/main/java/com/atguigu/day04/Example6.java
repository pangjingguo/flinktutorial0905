package com.atguigu.day04;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import com.atguigu.util.UserViewCountPerWindow;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// Example3的底层实现
public class Example6 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new MyTumblingProcessingTimeWindow(5000L))
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyTumblingProcessingTimeWindow extends KeyedProcessFunction<String, ClickEvent, UserViewCountPerWindow> {
        private long windowSize;

        public MyTumblingProcessingTimeWindow(long windowSize) {
            this.windowSize = windowSize;
        }

        // key: 窗口开始时间
        // value: 窗口的累加器
        private MapState<Long, Long> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "windowStartTime-accumulator",
                            Types.LONG,
                            Types.LONG
                    )
            );
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            long currentTimestamp = ctx.timerService().currentProcessingTime();
            long windowStartTime = currentTimestamp - currentTimestamp % windowSize;

            if (!mapState.contains(windowStartTime)) {
                mapState.put(windowStartTime, 1L);
            } else {
                mapState.put(windowStartTime, mapState.get(windowStartTime) + 1L);
            }

            ctx.timerService().registerProcessingTimeTimer(windowStartTime + windowSize - 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            long windowStartTime = timestamp + 1L - windowSize;
            long windowEndTime = timestamp + 1L;
            long count = mapState.get(windowStartTime);
            String username = ctx.getCurrentKey();
            out.collect(new UserViewCountPerWindow(username, count, windowStartTime, windowEndTime));

            mapState.remove(windowStartTime);
        }
    }
}
