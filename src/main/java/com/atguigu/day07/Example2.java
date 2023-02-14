package com.atguigu.day07;

import com.atguigu.util.Event;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

// SELECT * FROM A INNER JOIN B ON A.key=B.key;
public class Example2 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> leftStream = env
                .fromElements(
                        new Event("key-1", "left-1"),
                        new Event("key-2", "left-2"),
                        new Event("key-1", "left-3")
                );

        DataStreamSource<Event> rightStream = env
                .fromElements(
                        new Event("key-1", "right-1"),
                        new Event("key-2", "right-2"),
                        new Event("key-1", "right-3")
                );

        leftStream.keyBy(r -> r.key)
                .connect(rightStream.keyBy(r -> r.key))
                .process(new InnerJoin())
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class InnerJoin extends CoProcessFunction<Event, Event, String> {
        private ListState<Event> leftHistoryData;
        private ListState<Event> rightHistoryData;

        @Override
        public void open(Configuration parameters) throws Exception {
            leftHistoryData = getRuntimeContext().getListState(
                    new ListStateDescriptor<Event>(
                            "left-state",
                            Types.POJO(Event.class)
                    )
            );
            rightHistoryData = getRuntimeContext().getListState(
                    new ListStateDescriptor<Event>(
                            "right-state",
                            Types.POJO(Event.class)
                    )
            );
        }

        @Override
        public void processElement1(Event left, Context context, Collector<String> collector) throws Exception {
            // 每来一条数据，保存到列表状态中
            // 假设输入的数据是：Event("key-1", "left-1")
            // 将输入数据添加到key为"key-1"对应的leftHistoryData列表状态变量
            leftHistoryData.add(left);

            // 然后和另一条流所有相同key的历史数据作join
            // 访问的是key为"key-1"对应的列表状态变量
            for (var right : rightHistoryData.get())
                System.out.println(left + " ==> " + right);
        }

        @Override
        public void processElement2(Event right, Context context, Collector<String> collector) throws Exception {
            rightHistoryData.add(right);

            for (var left : leftHistoryData.get())
                System.out.println(left + " ==> " + right);
        }
    }
}
