package com.ilife.analyzer2.process;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ilife.analyzer2.entity.Fact;

/**
 * 将stuff/user json文本解析为按属性存储的Fact
 *
 */
public class JsonCsvParser extends ProcessFunction<String, String> {
	private static Logger logger = Logger.getLogger(JsonCsvParser.class);
    private static final long serialVersionUID = 1L;
    
    String ignoreList = "task,url,meta,title,summary,images,link,status,@timestamp,@version,categoryId,profit,location,index,logo,address,_key,type";//忽略的字段，不需要进行打散，后续不用于计算用途
    String[] inputFields = {"_doc"};//需要输入的字段，第一个必须是json字段
    String[] outfields = {"property","value","category","itemKey"};
    
    String itemKey = "";
    String platform = "";
    String category = "";
    String categoryId = "";

    List<String> buffer = Lists.newArrayList();
    
	@Override
	public void processElement(String json, ProcessFunction<String, String>.Context context, Collector<String> collector)
			throws Exception {
		JSONObject doc = (JSONObject)JSONObject.parse(json);

		if(doc.getJSONObject("meta") == null || 
				doc.getJSONObject("meta").getString("category") == null || 
				doc.getJSONObject("meta").getString("category").trim().length()==0) {
			logger.warn("no categoryId found. skip.[json]"+json);
			return;
		}
		
		logger.debug("*********got kafka stream.[json]"+json);
		
		//获取基础字段信息，包括itemKey/platform/category/categoryId
		itemKey = doc.getString("_key");
		platform = doc.getString("source");
		categoryId = doc.getJSONObject("meta")==null?"NA":doc.getJSONObject("meta").getString("category");
		if(doc.get("category") instanceof JSONArray && doc.getJSONArray("category").size()>0) {
			JSONArray arr = doc.getJSONArray("category");
			for(int i=0;i<arr.size();i++)
				category += " "+ arr.getString(i);
			category = category.trim();
		}else if(doc.get("category") instanceof JSONArray) {
			category = "";
		}else
			category = doc.getString("category");
		
		parse("",doc);
		
		for(String record:buffer) {
			logger.info("try to emit.[csv]"+record);
			collector.collect(record);
		}
	}
	

    /**
     * 解析Map数据，即键值对。
     * @param prefix：键值前缀
     * @param map：待解析Map数据
     * @param tuple：当前tuple，用于获取category、_key等固定字段
     */
    private void parse(String prefix,JSONObject json) {
    	if(prefix.trim().length()>0 && ignoreList.indexOf(prefix)>-1) {
    		logger.debug("ignore doc properties.[attr]"+prefix);
    		return;
    	}
		logger.debug("===map=== [prefix]"+prefix+"[json]"+json);
	    Iterator<Entry<String,Object>> iter= json.entrySet().iterator();
	    while(iter.hasNext()) {
	    		Entry<String,Object> entry = iter.next();
	    		String key = prefix.trim().length()==0?entry.getKey():prefix+"."+entry.getKey();
	    		logger.debug("process jsonobject.[key]"+key+
//	    				"\t[type]"+entry.getValue()==null?null:entry.getValue().getClass()+
	    				"\t[value]"+entry.getValue());
	    		if(entry.getValue() instanceof JSONArray) {//embed key:s array
	    			JSONArray list = (JSONArray)entry.getValue();
	    			//判定类型
	    			if(list.size()>0 && list.get(0)!=null && list.get(0) instanceof JSONObject) {//Map列表则逐个解析
	    				logger.debug("===map array item=== [prefix]"+prefix+"[map]"+entry.getValue());
		    			for(int i=0;i<list.size();i++) {
		    				JSONObject obj = list.getJSONObject(i);
		    				//parse(key+"."+(i++),m,tuple);//map数组内通过添加数字序列后缀，假设拥有相似的字段
		    				parse(key,obj);//注意：假设数组内的key值不同，如果数组内有相同key值，记录会被覆盖。
		    			}
	    			}else {
	    				logger.debug("===array item=== [prefix]"+prefix+"[value]"+entry.getValue());
	    				parse(key,entry.getValue());
	    			}
	    		}else if(entry.getValue() instanceof JSONObject) {//embed key:s
	    			JSONObject json2 = (JSONObject)entry.getValue();
	    			parse(key,json2);
	    		} else {
	    			parse(key,entry.getValue());
	    		}
	    }    	
    }
    
    /**
     * 解析单个数值，直接发送key:value对
     * @param key：数据键
     * @param value：数据值
     * @param tuple：当前tuple，用于获取category、_key等固定字段
     */
    private void parse(String key, Object value) {
    	if(key.trim().length()>0 && ignoreList.indexOf(key)>-1) //过滤掉不需要的字段
    		return;
    	if(null==value || (""+value).trim().length()==0 || (value instanceof List<?> && ((List) value).isEmpty()))//过滤掉空值
    		return;
		logger.debug("===value=== [key]"+key+"[value]"+value);
        try {
        	Fact fact = new Fact(itemKey,platform,category,categoryId);
        	fact.setProperty(key);
    		if(value instanceof List<?>) {//如果是数组，直接转换为空格分隔的字符串
    			List<Object> list = (List<Object>)value;
    			String strValue = "";
    			for(Object obj:list)
    				strValue += " "+obj;
    			fact.setOvalue(strValue.trim());//去掉第一个分隔符
    		}else
    			fact.setOvalue(""+value);
    		logger.debug(fact.toString());
    		buffer.add(Fact.toCsv(fact));
    		//collector.collect(Fact.toCsv(fact));
	    } catch (Exception e) {
	    	logger.error("failed parse fact from json.[json]"+value);
	    } 	
    }
	
}