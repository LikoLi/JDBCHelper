package org.liko.JDBCHelper;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.junit.Test;

public class JDBCHelperTest {
	@Test
	public void testQueryList() {
		try {
			String sql = "SELECT * FROM DIM_TRANSFER_TRANSFER";
			List<Map<String, Object>> queryList = JDBCHelper.queryList(sql);
			System.out.println(queryList);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
