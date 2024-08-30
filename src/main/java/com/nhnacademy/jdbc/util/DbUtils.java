package com.nhnacademy.jdbc.util;

import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;

public class DbUtils {
    private static final String DB_URL = "jdbc:mysql://133.186.241.167:3306/nhn_academy_125";
    private static final String DB_USER = "nhn_academy_125";
    private static final String DB_PASSWORD = "sA4aiwse!";

    public DbUtils(){
        throw new IllegalStateException("Utility class");
    }

    private static final DataSource DATASOURCE;

    static {
        BasicDataSource basicDataSource = new BasicDataSource();

        //#todo#0 {ip},{database},{username},{password} 설정
        basicDataSource.setUrl(DB_URL);
        basicDataSource.setUsername(DB_USER);
        basicDataSource.setPassword(DB_PASSWORD);

        basicDataSource.setInitialSize(5);
        basicDataSource.setMaxTotal(5);
        basicDataSource.setMaxIdle(5);
        basicDataSource.setMinIdle(5);

        basicDataSource.setMaxWait(Duration.ofSeconds(2));
        basicDataSource.setValidationQuery("select 1");
        basicDataSource.setTestOnBorrow(true);
        DATASOURCE = basicDataSource;
    }

    public static DataSource getDataSource(){
        return DATASOURCE;
    }

}