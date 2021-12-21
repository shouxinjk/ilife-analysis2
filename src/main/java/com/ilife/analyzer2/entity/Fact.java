package com.ilife.analyzer2.entity;

import java.util.Objects;
import java.util.Date;
import com.alibaba.fastjson.JSONObject;

public final class Fact {

    private String itemKey;//stuff/user标识
    private String platform;//来源平台
    private String category;//原始类目名称
    private String categoryId;//标准类目Id，非空，否则会直接丢弃
    private String property;//原始属性名称
    private String propertyKey="";//标注属性Key
    private String propertyId="";//标准属性ID
    private String ovalue;//原始值
    private int valueType=0;//0：单值，1：多值
    private String labelType="";//标注类型：manual/auto/dict/refer
    private String labelDict="";//dict标注字典表名称
    private String labelCategory="";//refer标注引用类目ID
    private String labelTagCategory="";//标签映射类目
    private String normalizeType="";//归一化类型：min-max/max-min/logx/logx-reverse/exp/exp-reverse
    private String multiValueFunc="";//多值计算类型：max/min/avg/sum
    private double mvalue=0;//标注值，初次加载默认为0
    private double score=0;//归一化得分，初始设置为0
    private double alpha=0.2;
    private double beta=0.2;
    private double gamma=0.2;
    private double delte=0.2;
    private double epsilon=0.2;
    private double zeta=0.4;
    private double eta=0.3;
    private double theta=0.3;
    private String lambda="";
    private int mstatus=0;//标注状态：0待标注、1已标注
    private int nstatus=0;//归一化状态:0待处理、1已完成
    private Date ts = new Date();//时间戳

    public Fact() {}

    public Fact(String itemKey,String platform,String category,String categoryId) {
    	this.itemKey=itemKey;
    	this.platform=platform;
    	this.category=category;
    	this.categoryId=categoryId;
    }
    
    public Fact(String itemKey,String platform,String category,String categoryId,String property,String propertyId,String ovalue) {
    	this.itemKey=itemKey;
    	this.platform=platform;
    	this.category=category;
    	this.categoryId=categoryId;
    	this.property=property;
    	this.propertyId=propertyId;
    	this.ovalue=ovalue;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Fact fact = (Fact) o;
        return itemKey == fact.itemKey && 
        		platform == fact.platform && 
        		category == fact.category && 
        		property == fact.property;
    }

    public static String toCsv(Fact fact) {
        StringBuilder builder = new StringBuilder();
        builder.append("('");
        builder.append(fact.itemKey);
        builder.append("', ");
 
        builder.append("'");
        builder.append(fact.platform);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.category);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.categoryId);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.property);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.propertyKey);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.propertyId);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.ovalue);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.valueType);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.labelType);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.labelDict==null?"":fact.labelDict);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.labelCategory==null?"":fact.labelCategory);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.labelTagCategory==null?"":fact.labelTagCategory);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.normalizeType);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.multiValueFunc==null?"":fact.multiValueFunc);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.mvalue);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.score);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.alpha);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.beta);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.gamma);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.delte);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.epsilon);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.zeta);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.eta);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.theta);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.lambda);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.mstatus);
        builder.append("', ");
        
        builder.append("'");
        builder.append(fact.nstatus);
        builder.append("', ");

        builder.append("toDateTime(now())");
        builder.append(")");
        return builder.toString();
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(itemKey,property);
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

	public String getPlatform() {
		return platform;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(String categoryId) {
		this.categoryId = categoryId;
	}

	public String getProperty() {
		return property;
	}

	public void setProperty(String property) {
		this.property = property;
	}

	public String getPropertyKey() {
		return propertyKey;
	}

	public void setPropertyKey(String propertyKey) {
		this.propertyKey = propertyKey;
	}

	public String getPropertyId() {
		return propertyId;
	}

	public void setPropertyId(String propertyId) {
		this.propertyId = propertyId;
	}

	public String getOvalue() {
		return ovalue;
	}

	public void setOvalue(String ovalue) {
		this.ovalue = ovalue;
	}

	public int getValueType() {
		return valueType;
	}

	public void setValueType(int valueType) {
		this.valueType = valueType;
	}

	public String getLabelType() {
		return labelType;
	}

	public void setLabelType(String labelType) {
		this.labelType = labelType;
	}

	public String getLabelDict() {
		return labelDict;
	}

	public void setLabelDict(String labelDict) {
		this.labelDict = labelDict;
	}

	public String getLabelCategory() {
		return labelCategory;
	}

	public void setLabelCategory(String labelCategory) {
		this.labelCategory = labelCategory;
	}

	public String getLabelTagCategory() {
		return labelTagCategory;
	}

	public void setLabelTagCategory(String labelTagCategory) {
		this.labelTagCategory = labelTagCategory;
	}

	public String getNormalizeType() {
		return normalizeType;
	}

	public void setNormalizeType(String normalizeType) {
		this.normalizeType = normalizeType;
	}

	public String getMultiValueFunc() {
		return multiValueFunc;
	}

	public void setMultiValueFunc(String multiValueFunc) {
		this.multiValueFunc = multiValueFunc;
	}

	public double getMvalue() {
		return mvalue;
	}

	public void setMvalue(double mvalue) {
		this.mvalue = mvalue;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public double getAlpha() {
		return alpha;
	}

	public void setAlpha(double alpha) {
		this.alpha = alpha;
	}

	public double getBeta() {
		return beta;
	}

	public void setBeta(double beta) {
		this.beta = beta;
	}

	public double getGamma() {
		return gamma;
	}

	public void setGamma(double gamma) {
		this.gamma = gamma;
	}

	public double getDelte() {
		return delte;
	}

	public void setDelte(double delte) {
		this.delte = delte;
	}

	public double getEpsilon() {
		return epsilon;
	}

	public void setEpsilon(double epsilon) {
		this.epsilon = epsilon;
	}

	public double getZeta() {
		return zeta;
	}

	public void setZeta(double zeta) {
		this.zeta = zeta;
	}

	public double getEta() {
		return eta;
	}

	public void setEta(double eta) {
		this.eta = eta;
	}

	public double getTheta() {
		return theta;
	}

	public void setTheta(double theta) {
		this.theta = theta;
	}

	public String getLambda() {
		return lambda;
	}

	public void setLambda(String lambda) {
		this.lambda = lambda;
	}

	public int getMstatus() {
		return mstatus;
	}

	public void setMstatus(int mstatus) {
		this.mstatus = mstatus;
	}

	public int getNstatus() {
		return nstatus;
	}

	public void setNstatus(int nstatus) {
		this.nstatus = nstatus;
	}

	public Date getTs() {
		return ts;
	}

	public void setTs(Date ts) {
		this.ts = ts;
	}

    
}
