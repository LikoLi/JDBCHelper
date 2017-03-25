package org.liko.JDBCHelper;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JDBCUtil {
	private static Log Logger = LogFactory.getLog(JDBCUtil.class); 
	private static String driver;
	private static String url;
	private static String username;
	private static String password;
	
	static {
		DBConfig dbConfig = new DBConfig();
		driver = dbConfig.getDriver();
		url = dbConfig.getUrl();
		username = dbConfig.getUsername();
		password = dbConfig.getPassword();
		
		try {
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			Logger.error("init jdbc driver fail!");
		}
	}
	
	public static Connection getConnection() throws SQLException {
		return DriverManager.getConnection(url, username, password);
	}
	
	private static void releaseConnection(Connection con) throws SQLException {
		if(con != null) {
			con.close();
		}
	}
	
	private static void releaseStatement(Statement statement) throws SQLException {
		if(statement != null) {
			statement.close();
		}
	}
	
	private static void releaseResultSet(ResultSet rs) throws SQLException {
		if(rs != null) {
			rs.close();
		}
	}
	
	public static void release(Connection con, Statement statement, ResultSet rs) throws SQLException {
		releaseConnection(con);
		releaseStatement(statement);
		releaseResultSet(rs);
	}
}
