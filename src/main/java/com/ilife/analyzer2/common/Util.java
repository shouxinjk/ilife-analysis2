package com.ilife.analyzer2.common;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.log4j.Logger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Util {
	private static final Logger logger = Logger.getLogger(Util.class);
	static String configfile = "ilife.properties";
	static String jdbcConfigFile = "jdbc.properties";
	
	static HikariDataSource datasource = null;

	public static Properties getConfig() {
		return getConfig("ilife.properties");
	}
	
	public static Properties getConfig(String filename) {
		final Properties props=new Properties();
		try {
			props.load(Util.class.getClassLoader().getResourceAsStream(filename));
		} catch (IOException e) {
			logger.error("failed load config from property file.[filename]"+filename);
		}
		return props;
	}
	
	public static HikariDataSource getDatasource() {
		if(datasource == null) {
			try (InputStream is = Util.class.getClassLoader().getResourceAsStream(jdbcConfigFile)) {
	            // 加载属性文件并解析：
	            Properties props = new Properties();
	            props.load(is);
	            HikariConfig config = new HikariConfig(props);
	            datasource= new HikariDataSource(config);
	            return datasource;
	        } catch (IOException e) {
	            logger.error("failed init data source.[err]"+e.getMessage());
	            return null;
	        }
		}else
			return datasource;
	}
	
	
}
