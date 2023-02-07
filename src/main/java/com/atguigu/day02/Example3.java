package com.atguigu.day02;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example3 {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .map(new MapFunction<ClickEvent, String>() {
                    @Override
                    public String map(ClickEvent clickEvent) throws Exception {
                        return clickEvent.username;
                    }
                })
                .print("匿名类");

        env
                .addSource(new ClickSource())
                .map(r -> r.username)
                .print("匿名函数");

        env
                .fromElements(1)
                .map(r -> Tuple2.of(r, r))
                // Tuple2<Integer, Integer> => Tuple2<Object, Object>
                .returns(Types.TUPLE(Types.INT, Types.INT))
                .print();

        env
                .addSource(new ClickSource())
                .map(new MyMap())
                .print("外部类");

        env
                .addSource(new ClickSource())
                .flatMap(new FlatMapFunction<ClickEvent, String>() {
                    @Override
                    public void flatMap(ClickEvent clickEvent, Collector<String> collector) throws Exception {
                        collector.collect(clickEvent.username);
                    }
                })
                .print("flatMap");

        env.execute();
    }

    public static class MyMap implements MapFunction<ClickEvent, String> {
        @Override
        public String map(ClickEvent clickEvent) throws Exception {
            return clickEvent.username;
        }
    }
}
