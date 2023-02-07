package com.atguigu.util;

import java.sql.Timestamp;

public class ClickEvent {
    public String username;
    public String url;
    public Long timestamp;

    public ClickEvent() {
    }

    public ClickEvent(String username, String url, Long timestamp) {
        this.username = username;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "用户名: " + username + ", url: " + url + ", 时间戳: " + new Timestamp(timestamp);
    }
}
