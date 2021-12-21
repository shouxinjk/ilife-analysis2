package com.ilife.analyzer2.common;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.ilife.analyzer2.entity.Info;

public class InfoDeserializer implements KafkaDeserializationSchema<Info> {
	private static final long serialVersionUID = -7682498573671412304L;
	private static final Logger logger = Logger.getLogger(InfoDeserializer.class);
	
	private final String encoding = "UTF8";
	
	@Override
	public TypeInformation<Info> getProducedType() {
		return TypeInformation.of(Info.class);
	}
	@Override
	public boolean isEndOfStream(Info nextElement) {
		return false;
	}
	@Override
	public Info deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		if (record != null) {
			String json = new String(record.value(), encoding);
			return JSONObject.parseObject(json, Info.class);
		}else
			logger.error("no record found.");
		return null;
	}
	
}



