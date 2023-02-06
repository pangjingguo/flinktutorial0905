package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 从socket读取数据，然后进行单词计数
public class Example1 {
    // 不要忘记抛出异常
    public static void main(String[] args) throws Exception {
        // 获取运行时上下文环境
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置数据源，并行任务的数量是1个
        // 先启动`nc -lk 9999`
        var source = env.socketTextStream("localhost", 9999).setParallelism(1);

        // map阶段
        // "hello world" => (hello, 1) (world, 1)
        // flatMap
        var mappedStream = source
                .flatMap(new Tokenizer())
                .setParallelism(1);

        // shuffle
        // 将元组的f0字段设置为key
        var keyedStream = mappedStream.keyBy(r -> r.f0);

        // reduce阶段
        // 每来一条数据，就更新一次累加器，然后将输入数据丢弃，并输入累加器的值
        // ReduceFunction只有一个泛型，因为输入、输出和累加器的类型相同
        var result = keyedStream
                .reduce(new WordCount())
                .setParallelism(1);

        // 输出
        result.print().setParallelism(1);

        // 提交程序
        env.execute();
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String input, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 集合out用来收集将要发送的数据
            // flink自动将集合中的数据发送下去
            var words = input.split(" ");
            for (var word : words)
                out.collect(Tuple2.of(word, 1));
        }
    }

    public static class WordCount implements ReduceFunction<Tuple2<String, Integer>> {
        // 返回值是新的累加器，会覆盖掉旧的累加器
        // reduce方法定义的是输入数据和累加器的聚合规则
        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> acc, Tuple2<String, Integer> input) throws Exception {
            return Tuple2.of(acc.f0, acc.f1 + input.f1);
        }
    }
}
