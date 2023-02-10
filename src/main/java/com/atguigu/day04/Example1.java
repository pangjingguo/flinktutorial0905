package com.atguigu.day04;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Example1 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .keyBy(r -> r.username)
                .process(new KeyedProcessFunction<String, ClickEvent, String>() {
                    // 声明一个字典状态变量
                    // KEY: url
                    // VALUE: url的访问次数
                    private MapState<String, Long> urlCount;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        urlCount = getRuntimeContext().getMapState(
                                new MapStateDescriptor<String, Long>(
                                        "url-count",
                                        Types.STRING,
                                        Types.LONG
                                )
                        );
                    }

                    @Override
                    public void processElement(ClickEvent in, Context ctx, Collector<String> out) throws Exception {
                        // 如果到达的数据是(Mary, ./home)
                        // 那么在Mary对应的字典状态变量中查询是否存在`./home`这个key
                        if (!urlCount.contains(in.url)) {
                            urlCount.put(in.url, 1L);
                        } else {
                            urlCount.put(in.url, urlCount.get(in.url) + 1L);
                        }

                        // 格式化输出`in.username`所对应的字典状态变量
                        var result = new StringBuilder();
                        result.append(ctx.getCurrentKey()).append(" => {\n");
                        for (var url : urlCount.keys()) {
                            result.append("  ").append(url).append(" -> ").append(urlCount.get(url)).append("\n");
                        }
                        result.append("}\n");

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
