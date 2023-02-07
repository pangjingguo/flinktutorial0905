package com.atguigu.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<ClickEvent> {
    private boolean running = true;
    private final String[] userArray = {"Mary", "Bob", "Alice"};
    private final String[] urlArray = {"./home", "./buy", "./cart"};
    private final Random random = new Random();
    @Override
    public void run(SourceContext<ClickEvent> ctx) throws Exception {
        while (running) {
            ctx.collect(new ClickEvent(
                    userArray[random.nextInt(userArray.length)],
                    urlArray[random.nextInt(urlArray.length)],
                    Calendar.getInstance().getTimeInMillis() // 毫秒级时间戳
            ));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
