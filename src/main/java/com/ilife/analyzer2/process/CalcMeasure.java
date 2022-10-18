package com.ilife.analyzer2.process;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import com.ilife.analyzer2.common.Util;
import com.ilife.analyzer2.entity.Info;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import redis.clients.jedis.Jedis;

/**
 * 根据定义的公式完成measure计算。
 * 
 * 处理逻辑：
 * 1，根据itemKey读取对应的属性kv列表，当前直接从mysql读取
 * 2，根据定义的公式用groovy完成计算，得到info.score值
 * 3，转换为csv格式返回
 *
 */
public class CalcMeasure extends ProcessFunction<Info, String> {
	private static Logger logger = Logger.getLogger(CalcMeasure.class);
    private static final long serialVersionUID = 1L;
    
    double defaultScore = 0.5;//默认参数值
    
    String ignoreVariables = ",script,weighted,sum,ignore";//原始脚本中有类似 script weighted-sum 等，需要排除
	Binding binding = null;//缓存对应itemKey的所有变量。TODO：注意，由于有优先级，缓存的变量将根据一个优先级而变化
    
	@Override
	public void processElement(Info info, ProcessFunction<Info, String>.Context context, Collector<String> collector)
			throws Exception {
		
		//默认所有参数设置为默认值：0.5
		setDefaultValues(info.getItemKey(),info.getScript());
		
		//根据itemKey读取所有可用属性列表：TODO 需要升级为redis，当前直接从mysql读取
		getVariables(info.getItemKey());
		
		//根据公式计算得分
		GroovyShell shell = new GroovyShell(binding);
		Object value = null;
		
		//groovy脚本计算
        try {
        	value = shell.evaluate(info.getScript());//返回：text
        	try {
        		info.setScore(Double.parseDouble(value.toString()));
        	}catch(Exception ex) {
        		info.setScore( 0.5);//默认设置为0.5
        	}
        	info.setStatus(1);//修改计算状态
        	//将当前计算结果作为后续计算的variable
        	binding.setVariable(info.getDimensionKey(), info.getScore());
        }catch(Exception ex) {//出错则不做任何修改
        	logger.error("failed eval script.[script]"+info.getScript());
        	if("dev".equalsIgnoreCase(Util.getConfig().get("common.mode").toString()))
        		info.setScore(0.7);//only for test
        }
        
        //设置加入缓存
        Jedis jedis = Util.getJedisCachePool().getResource();
        try {
        	jedis.hset(info.getItemKey(), info.getDimensionKey(), ""+info.getScore());
        	jedis.close();
        }catch(Exception ex) {//出错则不做任何修改
        	logger.error("failed cache data. [itemKey] "+info.getItemKey()+" [dimensionKey] "+info.getDimensionKey());
			if(jedis!=null)Util.getJedisCachePool().returnBrokenResource(jedis);
		}
		//转换为csv返回
		collector.collect(Info.toCsv(info));
	}
	
	//默认值设置，根据计算公式，解析得到参数，默认数值设为0.5
	//注意：默认值仅在内存中设置，将被redis缓存数据替换
	private void setDefaultValues(String itemKey, String script) {
		logger.debug("try to set default values.[script]"+script);
		if(binding == null)
			binding = new Binding();
		
		//从脚本中解析出参数列表：匹配以字符开头且长度超过3的参数。
		//默认均为 pxxxx*0.2+mxxxx*0.3+price.sale*0.1
		Pattern p=Pattern.compile("([a-z][A-Za-z0-9_\\.]{2,})"); 
		Matcher m=p.matcher(script); 
		while(m.find()) { //仅在发现后进行
			logger.debug("try to bind default value.[key]"+m.group(1));
			if(m.group(1).trim().length()>0 && ignoreVariables.indexOf(m.group(1))<0)
				binding.setVariable(m.group(1),0.5);
		}
	}
	
	//从缓存结果中获取最新数值
	private void getVariables(String itemKey){
		if(binding == null)
			binding = new Binding();

		//从缓存获取数值
		Jedis jedis = Util.getJedisCachePool().getResource();
		Map<String,String> cachedData = null;
		try{
			cachedData = jedis.hgetAll(itemKey);//按照itemKey缓存
			jedis.close();
		}catch(Exception ex) {
			if(jedis!=null)Util.getJedisCachePool().returnBrokenResource(jedis);
		}
		
		//绑定到binding
		if( cachedData != null) {
			for(String key: cachedData.keySet()) {
				try {
					binding.setVariable(key, Double.parseDouble(cachedData.get(key)));
				}catch(Exception ex) {
					logger.error("failed to query values by itemKey."+ex.getMessage());
					binding.setVariable(key, 0.5);//默认情况下直接用0.5
				}
			}
		}else {
			logger.error("cannot get cached data from redis.[itemKey] "+itemKey);
		}

	}
	
}