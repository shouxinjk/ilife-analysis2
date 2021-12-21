package com.ilife.analyzer2;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import com.ilife.analyzer2.common.InfoDeserializer;
import com.ilife.analyzer2.common.Util;
import com.ilife.analyzer2.entity.Fact;
import com.ilife.analyzer2.entity.Info;
import com.ilife.analyzer2.process.CalcMeasure;
import com.ilife.analyzer2.process.JsonCsvParser;
import com.ilife.analyzer2.process.JsonParser;

import ru.ivi.opensource.flinkclickhousesink.ClickHouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;

/**
 * 从kafka接收info数据，根据定义的公式进行计算。
 * source：kafka.info 假设已经根据优先级排序，优先级高的先计算
 * process：根据itemKey读取其所有property及已计算info的数值，并启动groovy脚本完成计算
 * sink：clickhouse.info。同时写入缓存。【未来启用redis完成】
 *
 */
public class Measure {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//本地调试UI
		if(Util.getConfig().get("common.mode").toString().equalsIgnoreCase("dev")) {
			Configuration conf = new Configuration();
			conf.setString("rest.bind-port", "8081");
		    env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		}
		
		env.getConfig().setGlobalJobParameters(
			ParameterTool.fromPropertiesFile(Util.class.getClassLoader().getResourceAsStream("ilife.properties"))
		);
		
		// 1.1 把kafka设置为source
//		env.enableCheckpointing(5000); // checkpoint every 5000 msecs
		KafkaSource<Info> json = KafkaSource.<Info>builder()
			.setBootstrapServers(Util.getConfig().getProperty("brokers"))
			.setTopics("info")
			.setGroupId("flink-api")
			.setStartingOffsets(OffsetsInitializer.earliest())
//			.setValueOnlyDeserializer(new SimpleStringSchema())
			.setDeserializer(KafkaRecordDeserializationSchema.of(new InfoDeserializer()))
			.build();
		
		DataStreamSource<Info> source = env.fromSource(json, WatermarkStrategy.noWatermarks(), "receive-info");
		
		DataStream<String> facts = source
			.process(new CalcMeasure())
			.name("calc-info");
		
		//clickhouse sink
		Properties props = Util.getConfig();
		props.put(ClickHouseSinkConst.TARGET_TABLE_NAME, "ilife.info");
		props.put("socket_timeout", Util.getConfig().getProperty("clickhouse.socket-timeout"));//重要：缺少socket_timeout会导致clickhouse连接超时
		if(!Util.getConfig().get("common.mode").toString().equalsIgnoreCase("production")) {
			props.put(ClickHouseSinkConst.MAX_BUFFER_SIZE, "10");//本地调试小批量写入查看结果
			props.put("socket_timeout", 60000);//重要：缺少socket_timeout会导致clickhouse连接超时
		}
		ClickHouseSink sink = new ClickHouseSink(props);
		
		facts.addSink(sink).name("upsert-info");
		
		if(Util.getConfig().get("common.mode").toString().equalsIgnoreCase("dev")) {
			facts.print().name("print-console");
		}

		// execute program
		env.execute("measure");
	}
}


