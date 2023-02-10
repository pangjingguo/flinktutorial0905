package com.atguigu.day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class Example7 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 思路：
        // 报警功能由定时器负责
        // 第一种情况：如果出现温度上升 且 不存在报警定时器 => 注册1s之后的报警定时器，并将报警定时器的时间戳保存
        // 第二种情况：如果出现温度下降 且 存在报警定时器   => 删除报警定时器
        // 第三种情况：如果出现温度上升 且 存在报警定时器 => 什么都不做
        // 第四种情况：如果出现温度下降 且 不存在报警定时器 => 什么都不做
        env
                .addSource(new SensorSource())
                .keyBy(r -> r.sensorId)
                .process(new KeyedProcessFunction<String, SensorReading, String>() {
                    // 保存上一次的温度值
                    private ValueState<Double> lastTemp;
                    // 保存定时器的时间戳
                    private ValueState<Long> timerTs;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastTemp = getRuntimeContext().getState(
                                new ValueStateDescriptor<Double>(
                                        "last-temp",
                                        Types.DOUBLE
                                )
                        );
                        timerTs = getRuntimeContext().getState(
                                new ValueStateDescriptor<Long>(
                                        "timer-ts",
                                        Types.LONG
                                )
                        );
                    }

                    @Override
                    public void processElement(SensorReading in, Context ctx, Collector<String> out) throws Exception {
                        // 判断最近一次的温度是否为空，并缓存上一次的温度
                        Double prevTemp = lastTemp.value();
                        // 保存本次温度
                        lastTemp.update(in.temperature);

                        if (prevTemp != null) {
                            // 温度上升 && 不存在报警定时器
                            if (in.temperature > prevTemp && timerTs.value() == null) {
                                long currentTimestamp = ctx.timerService().currentProcessingTime();
                                // 注册1s之后的定时器
                                ctx.timerService().registerProcessingTimeTimer(currentTimestamp + 1000L);
                                // 将定时器的时间戳保存下来
                                timerTs.update(currentTimestamp + 1000L);
                            }
                            // 温度下降 && 存在报警定时器
                            else if (in.temperature < prevTemp && timerTs.value() != null) {
                                // 从定时器队列中**手动**删除定时器
                                ctx.timerService().deleteProcessingTimeTimer(timerTs.value());
                                // 清空timerTs
                                timerTs.clear();
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "连续1s温度上升！");
                        // onTimer的执行会自动删除定时器，所以同样需要清空timerTs
                        timerTs.clear();
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class SensorSource implements SourceFunction<SensorReading> {
        private boolean running = true;
        private final Random random = new Random();
        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            while (running) {
                for (int i = 1; i < 4; i++) {
                    ctx.collect(new SensorReading(
                            "sensor_" + i,
                            random.nextDouble()
                    ));
                }
                Thread.sleep(300L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class SensorReading {
        public String sensorId;
        public Double temperature;

        public SensorReading() {
        }

        public SensorReading(String sensorId, Double temperature) {
            this.sensorId = sensorId;
            this.temperature = temperature;
        }

        @Override
        public String toString() {
            return "SensorReading{" +
                    "sensorId='" + sensorId + '\'' +
                    ", temperature=" + temperature +
                    '}';
        }
    }
}
