package com.atguigu.day01;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Example2 {
    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 如果算子没有单独设置并行度，那么算子的并行度是1
        env.setParallelism(1);

        var source = FileSource
                .forRecordStreamFormat(
                        new TextLineInputFormat(),
                        new Path("/home/zuoyuan/flinktutorial0905/src/main/resources/words.txt")
                )
                .build();

        env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "null")
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
