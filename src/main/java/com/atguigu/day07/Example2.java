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
                        new Event("key-1", "left-1", 1000L),
                        new Event("key-2", "left-2", 1000L),
                        new Event("key-1", "left-3", 1000L)
                );

        DataStreamSource<Event> rightStream = env
                .fromElements(
                        new Event("key-1", "right-1", 2000L),
                        new Event("key-2", "right-2", 2000L),
                        new Event("key-1", "right-3", 2000L)
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
            // ?????????????????????????????????????????????
            // ???????????????????????????Event("key-1", "left-1")
            // ????????????????????????key???"key-1"?????????leftHistoryData??????????????????
            leftHistoryData.add(left);

            // ?????????????????????????????????key??????????????????join
            // ????????????key???"key-1"???????????????????????????
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
