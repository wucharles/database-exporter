package org.db.export.common.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.tomcat.jdbc.pool.DataSourceFactory;
import org.db.export.common.Constants;

public final class DataSourceUtils {
	private static Map<Properties, DataSource> mapping = new HashMap<Properties, DataSource>(2, 1F);
	
	private DataSourceUtils() {
		;
	}
	
	public static Connection con(Properties jdbc) {
		try {
			if(!mapping.containsKey(jdbc)) {
				Properties prop = new Properties();
				prop.put("driverClassName", jdbc.getProperty(Constants.CONST_JDBC_DRIVER_CLASSNAME));
				prop.put("url", jdbc.getProperty(Constants.CONST_JDBC_URL));
				prop.put("username", jdbc.getProperty(Constants.CONST_JDBC_USERNAME));
				prop.put("password", jdbc.getProperty(Constants.CONST_JDBC_PASSWD));
				DataSource ds = new DataSourceFactory().createDataSource(prop);
				mapping.put(jdbc, ds);
			}
			
			return mapping.get(jdbc).getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static void closeQuietly(Connection con) {
		try {
			if(con != null) {
				con.close();
			}
		} catch (SQLException e) {
			;
		}
	}
	
	public static void closeQuietly(ResultSet rs) {
		try {
			if(rs != null) {
				rs.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void closeQuietly(Statement stat) {
		try {
			if(stat != null) {
				stat.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void commitQuietly(Connection con) {
		try {
			if(con != null) {
				con.commit();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
