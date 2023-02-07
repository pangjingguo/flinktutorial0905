package com.atguigu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example5 {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .fromElements("white", "black", "gray")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        if (s.equals("white")) {
                            collector.collect(s);
                        } else if (s.equals("black")) {
                            collector.collect(s);
                            collector.collect(s);
                        }
                    }
                })
                .print("匿名类");

        env
                .fromElements("white", "black", "gray")
                .flatMap(new MyFlatMap())
                .print("外部类");

        env
                .fromElements("white", "black", "gray")
                .flatMap((String s, Collector<String> collector) -> {
                    if (s.equals("white")) {
                        collector.collect(s);
                    } else if (s.equals("black")) {
                        collector.collect(s);
                        collector.collect(s);
                    }
                })
                // Java的类型擦除机制
                // 在编译成字节码时，会发生类型擦除
                // Collector<String> collector => Collector<Object> collector
                // 明确泛型的类型，做类型注解
                .returns(Types.STRING)
                .print("匿名函数");

        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            if (s.equals("white")) {
                collector.collect(s);
            } else if (s.equals("black")) {
                collector.collect(s);
                collector.collect(s);
            }
        }
    }
}
