package com.atguigu.util;

import java.sql.Timestamp;

public class UserBehavior {
    public String productId;
    public String type;
    public Long ts;

    public UserBehavior() {
    }

    public UserBehavior(String productId, String type, Long ts) {
        this.productId = productId;
        this.type = type;
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "productId='" + productId + '\'' +
                ", type='" + type + '\'' +
                ", ts=" + new Timestamp(ts) +
                '}';
    }
}
