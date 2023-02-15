package com.atguigu.day08;

import com.atguigu.util.ClickSource;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example1 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 每隔10秒钟保存一次检查点文件
        env.enableCheckpointing(10 * 1000L);
        // 配置文件夹的绝对路径: file:// + 文件夹的绝对路径
        // windows: file:\\ + 文件夹的绝对路径
        env.setStateBackend(new FsStateBackend("file:///home/zuoyuan/flinktutorial0905/src/main/resources/ckpts"));

        env.addSource(new ClickSource()).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
