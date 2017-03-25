package org.liko.JDBCHelper;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCHelper {
	private static Connection con = null;
	private static PreparedStatement preparedStatement = null;
	private static CallableStatement  callableStatement = null;
	
	public static List queryList(String sql) throws SQLException {
		ResultSet rs = null;
		try {
			getPreparedStatement(sql);
			rs = preparedStatement.executeQuery();
			return resultToListMap(rs);
		} finally {
			release(rs);
		}
	}
	
	public static List queryList(String sql, Object... paramters) throws SQLException {
		ResultSet rs = null;
		try {
			getPreparedStatement(sql);
			for(int i = 0; i < paramters.length; i++) {
				preparedStatement.setObject(i + 1, paramters[i]);
			}
			rs = preparedStatement.executeQuery();
			return resultToListMap(rs);
		} finally {
			release(rs);
		}
	}
	
	public static Object querySingle(String sql) throws SQLException {
		Object result = null;
		ResultSet rs = null;
		
		try {
			getPreparedStatement(sql);
			rs = preparedStatement.executeQuery();
			if(rs.next()) {
				result = rs.getObject(1);
			}
			return result;
		} finally {
			release(rs);
		}
	}
	
	public static Object querySingle(String sql, Object... paramters) throws SQLException {
		Object result = null;
		ResultSet rs = null;
		try {
			getPreparedStatement(sql);
			for(int i = 0; i < paramters.length; i++) {
				preparedStatement.setObject(i + 1, paramters[i]);
			}
			rs = preparedStatement.executeQuery();
			if(rs.next()) {
				result = rs.getObject(1);
			}
			return result;
		} finally {
			release(rs);
		}
	}
	
	public static int update(String sql) throws SQLException {
		try {
			getPreparedStatement(sql);
			return preparedStatement.executeUpdate();
		} finally {
			release();
		}
	}
	
	public static int update(String sql, Object... paramters) throws SQLException {
		try {
			getPreparedStatement(sql);
			for(int i = 0; i < paramters.length; i++) {
				preparedStatement.setObject(i + 1, paramters[i]);
			}
			return preparedStatement.executeUpdate();
		} finally {
			release();
		}
	}
	
	public static Object insertWithReturenPrimeKey(String sql) throws SQLException {
		ResultSet rs = null;
		Object result = null;
		
		try {
			con = JDBCUtil.getConnection();
			preparedStatement = con.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			preparedStatement.execute();
			rs = preparedStatement.getGeneratedKeys();
			if(rs.next()) {
				result = rs.getObject(1);
			}
			return result;
		} finally {
			release(con, preparedStatement, rs);
		}
	}
	
	public static Object insertWithReturenPrimeKey(String sql, Object... paramters) throws SQLException {
		ResultSet rs = null;
		Object result = null;
		
		try {
			con = JDBCUtil.getConnection();
			preparedStatement = con.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
			for(int i = 0; i < paramters.length; i++) {
				preparedStatement.setObject(i + 1, i);
			}
			preparedStatement.execute();
			rs = preparedStatement.getGeneratedKeys();
			if(rs.next()) {
				result = rs.getObject(1);
			}
			return result;
		} finally {
			release(con, preparedStatement, rs);
		}
	}


	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static List resultToListMap(ResultSet rs) throws SQLException {
		List list = new ArrayList();
		while(rs.next()) {
			ResultSetMetaData md = rs.getMetaData();
			Map map = new HashMap();
			for(int i = 1; i < md.getColumnCount(); i++) {
				map.put(md.getColumnLabel(i), rs.getObject(i));
			}
			list.add(map);
		}
		return list;
	}

	private static void getPreparedStatement(String sql) throws SQLException {
		con = JDBCUtil.getConnection();
		preparedStatement = con.prepareStatement(sql);
	}
	
	private static void release() throws SQLException {
		release(null);
	}
	
	private static void release(ResultSet rs) throws SQLException {
		JDBCUtil.release(con, preparedStatement, rs);
	}
	
	private static void release(Connection con, Statement statement, ResultSet rs) throws SQLException {
		JDBCUtil.release(con, statement, rs);
	}
}
