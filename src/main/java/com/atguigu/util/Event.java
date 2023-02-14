package com.atguigu.util;

import java.sql.Timestamp;

public class Event {
    public String key;
    public String type;
    public Long ts;

    public Event() {
    }

    public Event(String key, String type, Long ts) {
        this.key = key;
        this.type = type;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "Event{" +
                "key='" + key + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + ts + "" +
                '}';
    }
}
