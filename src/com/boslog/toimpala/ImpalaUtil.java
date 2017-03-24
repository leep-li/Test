package com.boslog.toimpala;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class ImpalaUtil {
    
	public static Properties getProperties(){
		Properties prop = new Properties();
		String property = System.getProperty("user.home");
		File file = new File(property + "/cob/conf/mix_COB.properties");
		try {
			InputStream in = new FileInputStream(file);
			//加载参数
			prop.load(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return prop;
	}
	
	
//    private static final String IMPALA_HOST = "10.240.2.83";
//    private static final String IMPALA_JDBC_PORT = "21050";
//    private static final String CONNECTION_URL = "jdbc:impala://" + IMPALA_HOST + ":" + IMPALA_JDBC_PORT + "/;";
//    private static final String JDBC_DRIVER_NAME = "com.cloudera.impala.jdbc41.Driver";

    static {
        // 注册驱动
        try {
        	Properties prop = ImpalaUtil.getProperties();
        	String JDBC_DRIVER_NAME = prop.getProperty("JDBC_DRIVER_NAME");
        	
            Class.forName(JDBC_DRIVER_NAME);
            
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 获得连接
    public static Connection getConnection() throws Exception {
    	
    	Properties prop = ImpalaUtil.getProperties();
    	String CONNECTION_URL = prop.getProperty("CONNECTION_URL");
    	
        // 获得连接
        Connection conn = DriverManager.getConnection(CONNECTION_URL);
        
        return conn;
    }

    // 释放资源
    public static void release(Connection conn, Statement stmt, ResultSet rs) {
        // 释放资源
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        rs = null;
        release(conn, stmt);
    }
    // 释放资源
    public static void release(Connection conn, Statement stmt) {
        // 释放资源
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        stmt = null;

        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        conn = null;
    }
    
}
