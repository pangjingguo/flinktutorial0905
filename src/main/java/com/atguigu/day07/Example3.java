package com.atguigu.day07;

import com.atguigu.util.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

// leftStream和rightStream的对账
public class Example3 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        var leftStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> scx) throws Exception {
                        scx.collectWithTimestamp(new Event("key-1", "left", 1000L), 1000L);
                        Thread.sleep(1000L);
                        scx.collectWithTimestamp(new Event("key-2", "left", 2000L), 2000L);
                        Thread.sleep(1000L);
                        scx.emitWatermark(new Watermark(10 * 1000L));
                        Thread.sleep(2000L);
                    }

                    @Override
                    public void cancel() {
                    }
                });

        var rightStream = env
                .addSource(new SourceFunction<Event>() {
                    @Override
                    public void run(SourceContext<Event> scx) throws Exception {
                        scx.collectWithTimestamp(new Event("key-3", "right", 4000L), 4000L);
                        Thread.sleep(1000L);
                        scx.emitWatermark(new Watermark(20 * 1000L));
                        Thread.sleep(1000L);
                        scx.collectWithTimestamp(new Event("key-1", "right", 300 * 1000L), 300 * 1000L);
                        Thread.sleep(1000L);
                    }

                    @Override
                    public void cancel() {
                    }
                });

        leftStream.keyBy(r -> r.key)
                .connect(rightStream.keyBy(r -> r.key))
                .process(new Match())
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Match extends CoProcessFunction<Event, Event, String> {
        // 如果第一条流的事件先到达，那么保存下来
        private ValueState<Event> leftState;
        // 如果第二条流的事件先到达，那么保存下来
        private ValueState<Event> rightState;
        @Override
        public void open(Configuration parameters) throws Exception {
            leftState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>(
                            "left-state",
                            Types.POJO(Event.class)
                    )
            );
            rightState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Event>(
                            "right-state",
                            Types.POJO(Event.class)
                    )
            );
        }

        @Override
        public void processElement1(Event leftEvent, Context context, Collector<String> collector) throws Exception {
            if (rightState.value() == null) {
                // 说明leftEvent先到达
                leftState.update(leftEvent); // 保存下来等待对应的相同key的rightEvent
                context.timerService().registerEventTimeTimer(leftEvent.ts + 5000L);
            } else {
                collector.collect(leftEvent.key + "对账成功，right事件先到达。");
                rightState.clear();
            }
        }

        @Override
        public void processElement2(Event rightEvent, Context context, Collector<String> collector) throws Exception {
            if (leftState.value() == null) {
                rightState.update(rightEvent);
                context.timerService().registerEventTimeTimer(rightEvent.ts + 5000L);
            } else {
                collector.collect(rightEvent.key + "对账成功，left事件先到达。");
                leftState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            if (leftState.value() != null) {
                out.collect(leftState.value().key + "对账失败，right事件没到");
                leftState.clear();
            }
            if (rightState.value() != null) {
                out.collect(rightState.value().key + "对账失败，left事件没到");
                rightState.clear();
            }
        }
    }
}
