package org.liko.JDBCHelper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DBConfig {
	private Log logger = LogFactory.getLog(DBConfig.class);
	private String driver;
	private String url;
	private String username;
	private String password;
	
	public DBConfig() {
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("JDBC.PROPERTIES");
		Properties properties = new Properties();
		try {
			properties.load(inputStream);
			this.driver = properties.getProperty("jdbc.driver");
			this.url = properties.getProperty("jdbc.url");
			this.username = properties.getProperty("jdbc.username");
			this.password = properties.getProperty("jdbc.password");
		} catch (IOException e) {
			logger.error("load jdbc.properties fail!");
		}
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public String toString() {
		return "DBConfig [driver=" + driver + ", url=" + url + ", username=" + username
				+ ", password=" + password + "]";
	}
	
}
