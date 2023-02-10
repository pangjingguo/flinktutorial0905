package com.atguigu.day04;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import com.atguigu.util.UserViewCountPerWindow;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

// Example2的底层实现
public class Example5 {
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

        // MapState<Tuple2<窗口开始时间，窗口结束时间>, 窗口中元素组成的列表>
        private MapState<Tuple2<Long, Long>, List<ClickEvent>> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "windowInfo-elements",
                            Types.TUPLE(Types.LONG, Types.LONG),
                            Types.LIST(Types.POJO(ClickEvent.class))
                    )
            );
        }

        @Override
        public void processElement(ClickEvent in, Context ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // 输入数据的机器时间戳
            long currentTimestamp = ctx.timerService().currentProcessingTime();
            // 计算出输入数据所属的窗口的开始时间
            long windowStartTime = currentTimestamp - currentTimestamp % windowSize;
            // 计算窗口结束时间
            long windowEndTime = windowStartTime + windowSize;
            // 构建窗口信息
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);

            // 判断输入数据是否是这个窗口的第一条数据
            if (!mapState.contains(windowInfo)) {
                var elements = new ArrayList<ClickEvent>();
                elements.add(in);
                mapState.put(windowInfo, elements);
            } else {
                mapState.get(windowInfo).add(in);
            }

            // 注册(窗口结束时间 - 1毫秒)的定时器
            ctx.timerService().registerProcessingTimeTimer(windowEndTime - 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<UserViewCountPerWindow> out) throws Exception {
            // timestamp == windowEndTime - 1L
            long windowEndTime = timestamp + 1L;
            long windowStartTime = windowEndTime - windowSize;
            Tuple2<Long, Long> windowInfo = Tuple2.of(windowStartTime, windowEndTime);
            var username = ctx.getCurrentKey();
            var count = (long)mapState.get(windowInfo).size();

            out.collect(new UserViewCountPerWindow(username, count, windowStartTime, windowEndTime));

            // 在MapState中将窗口销毁
            mapState.remove(windowInfo);
        }
    }
}
