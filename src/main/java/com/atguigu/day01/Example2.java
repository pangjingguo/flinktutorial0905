package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example2 {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 将所有的算子的并行子任务的数量都设置为1
        env.setParallelism(1);

        env
                .readTextFile("/home/zuoyuan/flinktutorial0905/src/main/resources/words.txt")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        var words = input.split(" ");
                        for (var word : words) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                // 每来一条数据，对数据指定key，然后发送
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> input) throws Exception {
                        return input.f0; // 将f0字段指定为输入数据的key
                    }
                })
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> input) throws Exception {
                        return Tuple2.of(accumulator.f0, accumulator.f1 + input.f1);
                    }
                })
                .print();

        env.execute();
    }
}
