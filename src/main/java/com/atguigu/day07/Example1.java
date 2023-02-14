package com.atguigu.day07;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class Example1 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 点击事件流
        var clickStream = env.addSource(new ClickSource());

        // socket流，在socket中输入用户名，将clickStream中该用户名的数据输出
        var queryStream = env.socketTextStream("localhost", 9999);

        // 连接两条流
        clickStream
                // 在广播queryStream前将并行度设置为1,为什么？
                // 因为数据必须按照顺序广播出去
                .connect(queryStream.setParallelism(1).broadcast())
                .flatMap(new Query())
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Query implements CoFlatMapFunction<ClickEvent, String, ClickEvent> {
        private String queryString = "";

        @Override
        public void flatMap1(ClickEvent clickEvent, Collector<ClickEvent> collector) throws Exception {
            if (clickEvent.username.equals(queryString)) collector.collect(clickEvent);
        }

        @Override
        public void flatMap2(String s, Collector<ClickEvent> collector) throws Exception {
            queryString = s;
        }
    }
}
