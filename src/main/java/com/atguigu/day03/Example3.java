package com.atguigu.day03;

import com.atguigu.util.ClickEvent;
import com.atguigu.util.ClickSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

// 写入mysql数据库
// create database userbehavior;
// use userbehavior;
// create table clicks (username varchar(100), url varchar(100));
public class Example3 {
    public static void main(String[] args) {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        env
                .addSource(new ClickSource())
                .addSink(new MyJDBC());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyJDBC extends RichSinkFunction<ClickEvent> {
        private Connection connection;
        private PreparedStatement insertStatement;
        private PreparedStatement updateStatement;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/userbehavior?useSSL=false",
                    "root",
                    "root"
            );
            insertStatement = connection.prepareStatement(
                    "INSERT INTO clicks (username, url) VALUES (?, ?)"
            );
            updateStatement = connection.prepareStatement(
                    "UPDATE clicks SET url = ? WHERE username = ?"
            );
        }

        // 幂等写入mysql
        @Override
        public void invoke(ClickEvent in, Context context) throws Exception {
            // 首先进行更新操作
            updateStatement.setString(1, in.url);
            updateStatement.setString(2, in.username);
            updateStatement.execute();

            // 如果更新的行数为0
            if (updateStatement.getUpdateCount() == 0) {
                // 说明没有in.username对应的数据存在
                insertStatement.setString(1, in.username);
                insertStatement.setString(2, in.url);
                insertStatement.execute();
            }
        }

        @Override
        public void close() throws Exception {
            insertStatement.close();
            updateStatement.close();
            connection.close();
        }
    }
}
