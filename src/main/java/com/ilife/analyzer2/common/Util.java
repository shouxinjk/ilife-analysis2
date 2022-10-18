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

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class Util {
	private static final Logger logger = Logger.getLogger(Util.class);
	static String configfile = "ilife.properties";
	static String jdbcConfigFile = "jdbc.properties";
	static String redisConfigFile = "redis.properties";
	
	//jdbc连接池
	static HikariDataSource datasource = null;
	
	//redis连接池
	private static JedisPool jedisPool = null;

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
	        } catch (IOException e) {
	            logger.error("failed init data source.",e);
	            return null;
	        }
		}
		return datasource;
	}
	
    public static JedisPool getJedisCachePool(){
    	if(jedisPool == null) {
			try (InputStream is = Util.class.getClassLoader().getResourceAsStream(redisConfigFile)) {
	            // 加载属性文件并解析：
	            Properties prop = new Properties();
	            prop.load(is);
	            logger.error("got redis props."+is);
	            
	            JedisPoolConfig config = new JedisPoolConfig();
	            config.setMaxTotal(Integer.parseInt(prop.getProperty("redis.pool.maxActive")));
	            config.setMaxIdle(Integer.parseInt(prop.getProperty("redis.pool.maxIdle")));
	            config.setMinIdle(Integer.parseInt(prop.getProperty("redis.pool.minIdle")));
	            config.setMaxWaitMillis(Integer.parseInt(prop.getProperty("redis.pool.maxWait")));
	            config.setTestOnBorrow(true);
	            config.setTestOnReturn(true);
	            config.setTestWhileIdle(true);
	            String host = prop.getProperty("redis.host");
	            String port = prop.getProperty("redis.port");
	            String timeOut = prop.getProperty("redis.timeout");
	            String password = prop.getProperty("redis.password");
	            jedisPool = new JedisPool(config, host, Integer.parseInt(port), Integer.parseInt(timeOut),password);
	            logger.info("Initialize redis connection pool success.");
	        } catch (IOException e) {
	        	logger.error("Error occured while initializing redis connection pool. ",e);
	            return null;
	        }
    	}
        return jedisPool;
    }
	
}
