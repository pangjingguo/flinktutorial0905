package com.atguigu.day04;

import com.atguigu.util.IntegerSource;
import com.atguigu.util.IntegerStatistic;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Example4 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntegerSource())
                .keyBy(r -> "integer")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<Integer, IntegerStatistic, IntegerStatistic>() {
                            @Override
                            public IntegerStatistic createAccumulator() {
                                return new IntegerStatistic(Integer.MIN_VALUE, Integer.MAX_VALUE, 0, 0, 0);
                            }

                            @Override
                            public IntegerStatistic add(Integer in, IntegerStatistic acc) {
                                return new IntegerStatistic(
                                        Math.max(in, acc.max),
                                        Math.min(in, acc.min),
                                        acc.count + 1,
                                        acc.sum + in,
                                        (acc.sum + in) / (acc.count + 1)
                                );
                            }

                            @Override
                            public IntegerStatistic getResult(IntegerStatistic acc) {
                                return acc;
                            }

                            @Override
                            public IntegerStatistic merge(IntegerStatistic integerStatistic, IntegerStatistic acc1) {
                                return null;
                            }
                        },
                        new ProcessWindowFunction<IntegerStatistic, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<IntegerStatistic> elements, Collector<String> out) throws Exception {
                                out.collect(elements.iterator().next() + "，窗口：" + new Timestamp(context.window().getStart()) + "" +
                                        "~~" + new Timestamp(context.window().getEnd()));
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
