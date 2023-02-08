package com.atguigu.day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example2 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4)
                .setParallelism(1)
                .map(new RichMapFunction<Integer, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("map的索引为" + getRuntimeContext().getIndexOfThisSubtask() + "的并行子任务生命周期开始！");
                    }

                    @Override
                    public String map(Integer in) throws Exception {
                        return "map的索引为" + getRuntimeContext().getIndexOfThisSubtask() + "的并行子任务处理的数据是：" + in;
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("map的索引为" + getRuntimeContext().getIndexOfThisSubtask() + "的并行子任务生命周期结束！");
                    }
                })
                .setParallelism(2)
                .print()
                .setParallelism(2);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
