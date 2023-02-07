package com.atguigu.util;

public class IntegerStatistic {
    public Integer max;
    public Integer min;
    public Integer count;
    public Integer sum;
    public Integer avg;

    public IntegerStatistic() {
    }

    public IntegerStatistic(Integer max, Integer min, Integer count, Integer sum, Integer avg) {
        this.max = max;
        this.min = min;
        this.count = count;
        this.sum = sum;
        this.avg = avg;
    }

    @Override
    public String toString() {
        return "IntegerStatistic{" +
                "max=" + max +
                ", min=" + min +
                ", count=" + count +
                ", sum=" + sum +
                ", avg=" + avg +
                '}';
    }
}
