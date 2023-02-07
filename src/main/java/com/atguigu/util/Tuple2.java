package com.atguigu.util;

public class Tuple2<T0, T1> {
    public T0 f0;
    public T1 f1;

    public Tuple2() {
    }

    public Tuple2(T0 f0, T1 f1) {
        this.f0 = f0;
        this.f1 = f1;
    }

    public static <T0, T1> Tuple2<T0, T1> of(T0 f0, T1 f1) {
        return new Tuple2<>(f0, f1);
    }

    @Override
    public String toString() {
        return "(" + f0 + ", " + f1 + ")";
    }

    public static void main(String[] args) {
        var tuple1 = new Tuple2<String, String>("name", "atguigu");
        var tuple2 = Tuple2.of("name", "atguigu");
        System.out.println(tuple2);
    }
}
