package com.ilife.analyzer2.process;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.flink.shaded.curator4.org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import com.ilife.analyzer2.common.Util;
import com.ilife.analyzer2.entity.Info;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;

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
    
	static String cachedItemKey = "-";//缓存处理的itemKey
	Binding binding = null;//缓存对应itemKey的所有变量。TODO：注意，由于有优先级，缓存的变量将根据一个优先级而变化
    
	@Override
	public void processElement(Info info, ProcessFunction<Info, String>.Context context, Collector<String> collector)
			throws Exception {
		
		//根据itemKey读取所有可用属性列表：TODO 需要升级为redis，当前直接从mysql读取
		getVariables(info.getItemKey());
		
		//根据公式计算得分
		GroovyShell shell = new GroovyShell(binding);
		Object value = null;
        try {
        	value = shell.evaluate(info.getScript());//返回：text
        	info.setScore(Double.parseDouble(value.toString()));
        	info.setStatus(1);//修改计算状态
        	//将当前计算结果作为后续计算的variable
        	binding.setVariable(info.getDimensionKey(), info.getScore());
        }catch(Exception ex) {//出错则不做任何修改
        	logger.error("failed eval script.[script]"+info.getScript());
        	if(Util.getConfig().get("common.mode").toString().equalsIgnoreCase("dev"))
        		info.setScore(0.7);//only for test
        }
        
		//转换为csv返回
		collector.collect(Info.toCsv(info));
	}
	
	private void getVariables(String itemKey){
		if(cachedItemKey.equalsIgnoreCase(itemKey))//同一个itemKey仅需获取一次
			return;
		cachedItemKey = itemKey;//切换itemKey并重新装载数据
		binding = new Binding();
		try {
			ResultSet rs = Util.getDatasource().getConnection().prepareCall("select propKey,score from mod_variable where itemKey='+itemKey+'").executeQuery();
			while(rs.next()) {
				binding.setVariable(rs.getString("propKey"), rs.getDouble("score"));
			}
			rs.close();
		} catch (SQLException e) {
			logger.error("failed to query values by itemKey.[error]"+e.getMessage());
		}
	}
	
}