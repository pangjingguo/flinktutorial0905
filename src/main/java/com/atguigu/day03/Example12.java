package com.atguigu.day03;

import com.atguigu.util.IntegerSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

public class Example12 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new IntegerSource())
                .keyBy(r -> r % 2)
                .process(new KeyedProcessFunction<Integer, Integer, String>() {
                    private ListState<Integer> history;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        history = getRuntimeContext().getListState(
                                new ListStateDescriptor<>(
                                        "history",
                                        Types.INT
                                )
                        );
                    }

                    @Override
                    public void processElement(Integer in, Context ctx, Collector<String> out) throws Exception {
                        history.add(in);

                        // 将数据取出并排序
                        var list = new ArrayList<Integer>();
                        for (var i : history.get()) {
                            list.add(i);
                        }
                        list.sort(new Comparator<Integer>() {
                            @Override
                            public int compare(Integer i1, Integer i2) {
                                return i1 - i2;
                            }
                        });

                        // 格式化输出
                        var result = new StringBuilder();
                        if (ctx.getCurrentKey() == 0) {
                            result.append("偶数历史数据：");
                        } else {
                            result.append("奇数历史数据：");
                        }

                        for (var i : list) {
                            result.append(i).append(" ==> ");
                        }

                        out.collect(result.toString());
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
