package com.exercise.clickhouse;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class App {
    public static void main(String[] args) throws Exception {
        Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
        Connection connection = DriverManager.getConnection("jdbc:clickhouse://10.0.50.100:8123");

        Statement statement = connection.createStatement();
//        statement.executeQuery("create table newdb.jdbc_example(day Date, name String, age UInt8) Engine=Log");
        String sql = "desc newdb.jdbc_example";
        ResultSet rs = statement.executeQuery(sql);

        while (rs.next()) {
            // ResultSet 的下标值从 1 开始，不可使用 0，否则越界，报 ArrayIndexOutOfBoundsException 异常
            System.out.println();
        }


    }
}
