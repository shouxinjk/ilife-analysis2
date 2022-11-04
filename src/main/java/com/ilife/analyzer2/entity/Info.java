package com.ilife.analyzer2.entity;

import java.util.Objects;
import java.util.Date;
import com.alibaba.fastjson.JSONObject;

public final class Info {

    private String itemKey;//stuff/user标识
    private String categoryId;//标准类目Id，非空，否则会直接丢弃
    private String dimensionId;//原始属性名称
    private String dimensionKey="";//标注属性Key
    private int dimensionType=0;//标准属性ID
    private int priority=1;//原始值
    private int feature=0;//0：单值，1：多值
    private double weight=0;//标注值，初次加载默认为0
    private double score=0;//归一化得分，初始设置为0
    private String script="";
    private int status=1;//标注状态：0待标注、1已标注 经过分析系统处理后认为已完成
    private Date ts = new Date();//时间戳

    public Info() {}

    public Info(String itemKey,String dimensionId) {
    	this.itemKey=itemKey;
    	this.dimensionId=dimensionId;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Info fact = (Info) o;
        return itemKey == fact.itemKey && 
        		dimensionId == fact.dimensionId;
    }

    public static String toCsv(Info fact) {
        StringBuilder builder = new StringBuilder();
        builder.append("('");
        builder.append(fact.itemKey);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.categoryId);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.dimensionId);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.dimensionKey);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.dimensionType);
        builder.append("', ");
        
        builder.append("");
        builder.append(fact.priority);
        builder.append(", ");
        
        builder.append("");
        builder.append(fact.feature);
        builder.append(", ");
        
        builder.append("");
        builder.append(fact.weight);
        builder.append(", ");
        
        builder.append("'");
        builder.append(fact.script);
        builder.append("', ");

        builder.append("");
        builder.append(fact.score);
        builder.append(", ");
        
        builder.append("");
        builder.append(fact.status);
        builder.append(", ");

        builder.append("toDateTime(now())");
        builder.append(")");
        return builder.toString();
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(itemKey,dimensionId);
    }

    @Override
    public String toString() {
    	return JSONObject.toJSONString(this);
    }

	public String getItemKey() {
		return itemKey;
	}

	public void setItemKey(String itemKey) {
		this.itemKey = itemKey;
	}

	public String getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(String categoryId) {
		this.categoryId = categoryId;
	}

	public String getDimensionId() {
		return dimensionId;
	}

	public void setDimensionId(String dimensionId) {
		this.dimensionId = dimensionId;
	}

	public String getDimensionKey() {
		return dimensionKey;
	}

	public void setDimensionKey(String dimensionKey) {
		this.dimensionKey = dimensionKey;
	}

	public int getDimensionType() {
		return dimensionType;
	}

	public void setDimensionType(int dimensionType) {
		this.dimensionType = dimensionType;
	}

	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public int getFeature() {
		return feature;
	}

	public void setFeature(int feature) {
		this.feature = feature;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public Date getTs() {
		return ts;
	}

	public void setTs(Date ts) {
		this.ts = ts;
	}

    
}
