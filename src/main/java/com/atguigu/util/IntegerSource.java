package com.atguigu.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class IntegerSource implements SourceFunction<Integer> {
    private boolean running = true;
    private final Random random = new Random();
    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (running) {
            ctx.collect(random.nextInt(1000));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
