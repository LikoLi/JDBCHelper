package org.liko.JDBCHelper;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JDBCHelper {
	private static Connection con = null;
	private static PreparedStatement preparedStatement = null;
	private static CallableStatement  callableStatement = null;
	
	public static List<Map<String, Object>> queryList(String sql) throws SQLException {
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
	
	public static List callableQuery(String procedureSql) throws SQLException {
		ResultSet rs = null;
		try {
			getCallableStatement(procedureSql);
			rs = callableStatement.executeQuery();
			return resultToListMap(rs);
		} finally {
			release(rs, callableStatement);
		}
	}
	
	public static List callableQuery(String procedureSql, Object... paramters) throws SQLException {
		ResultSet rs = null;
		try {
			getCallableStatement(procedureSql);
			for(int i = 0; i < paramters.length; i++) {
				callableStatement.setObject(i + 1, paramters[i]);
			}
			rs = callableStatement.executeQuery();
			return resultToListMap(rs);
		} finally {
			release(rs, callableStatement);
		}
	}
	
	public static Object callableQuerySingle(String procedureSql) throws SQLException {
		Object result = null;
		ResultSet rs = null;
		try {
			getCallableStatement(procedureSql);
			rs = callableStatement.executeQuery();
			while(rs.next()) {
				result = rs.getObject(1);
			}
			return result;
		} finally {
			release(rs, callableStatement);
		}
	}
	
	public static Object callableQuerySingle(String procedureSql, Object... paramters) throws SQLException {
		Object result = null;
		ResultSet rs = null;
		try {
			getCallableStatement(procedureSql);
			for(int i = 0; i < paramters.length; i++) {
				callableStatement.setObject(i + 1, paramters[i]);
			}
			rs = callableStatement.executeQuery();
			while(rs.next()) {
				result = rs.getObject(1);
			}
			return result;
		} finally {
			release(rs, callableStatement);
		}
	}
	
	public static Object callableWithParamters(String procedureSql) throws SQLException {
		try {
			getCallableStatement(procedureSql);
			callableStatement.registerOutParameter(0, Types.OTHER);
			callableStatement.execute();
			return callableStatement.getObject(0);
		} finally {
			release(null, callableStatement);
		}
	}
	
	public static int callableUpdate(String procedureSql) throws SQLException {
		try {
			getCallableStatement(procedureSql);
			return callableStatement.executeUpdate();
		} finally {
			release(null, callableStatement);
		}
	}
	
	public static int callableUpdate(String procedureSql, Object... parameters) throws SQLException {
		try {
			getCallableStatement(procedureSql);
			for(int i = 0; i < parameters.length; i++) {
				callableStatement.setObject(i + 1, parameters[i]);
			}
			return callableStatement.executeUpdate();
		} finally {
			release(null, callableStatement);
		}
	}
	
	public static int[] batchUpdate(List<String> sqlList) throws SQLException {
		int[] result = new int[]{};
		Statement statement = null;
		try {
			con = JDBCUtil.getConnection();
			con.setAutoCommit(false);
			statement = con.createStatement();
			for(String sql : sqlList) {
				statement.addBatch(sql);
			}
			result = statement.executeBatch();
			con.commit();
		} finally {
			release(con, statement, null);
		}
		return result;
	}


	private static void getCallableStatement(String procedureSql) throws SQLException {
		con = JDBCUtil.getConnection();
		callableStatement = con.prepareCall(procedureSql);
	}

	private static List<Map<String, Object>> resultToListMap(ResultSet rs) throws SQLException {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		while(rs.next()) {
			ResultSetMetaData md = rs.getMetaData();
			Map<String, Object> map = new HashMap<String, Object>();
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
		release(con, preparedStatement, rs);
	}
	
	private static void release(ResultSet rs, Statement statement) throws SQLException {
		release(con, statement, rs);
	}
	
	private static void release(Connection con, Statement statement, ResultSet rs) throws SQLException {
		JDBCUtil.release(con, statement, rs);
	}
}
