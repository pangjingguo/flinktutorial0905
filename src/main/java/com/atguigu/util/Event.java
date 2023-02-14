package com.atguigu.util;

public class Event {
    public String key;
    public String type;

    public Event() {
    }

    public Event(String key, String type) {
        this.key = key;
        this.type = type;
    }

    @Override
    public String toString() {
        return "Event{" +
                "key='" + key + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
