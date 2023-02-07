package com.atguigu.day02;

import org.apache.flink.util.MathUtils;

// keyBy的底层算法
public class Example9 {
    public static void main(String[] args) {
        int key = 2;
        // 第一步，获取key的hashCode
        int hashCode = Integer.hashCode(key);
        // 第二步，根据hashCode计算出Murmurhash值
        int murmurHash = MathUtils.murmurHash(hashCode);
        // 默认的最大并行度是128
        // reduce的并行度是4
        // 计算key要发送到的reduce的子任务索引
        int idx = (murmurHash % 128) * 4 / 128;
        System.out.println(idx);
    }
}
