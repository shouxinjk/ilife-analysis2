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

import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import com.ilife.analyzer2.common.Util;
import com.ilife.analyzer2.entity.Fact;
import com.ilife.analyzer2.process.ItemJsonCsvParser;
import com.ilife.analyzer2.process.JsonParser;
import com.ilife.analyzer2.process.UserJsonCsvParser;

import ru.ivi.opensource.flinkclickhousesink.ClickHouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickHouseSinkConst;

/**
 * 从kafka接收user数据，根据props打散为fact数据。
 * source：kafka.user
 * process：解析json，输出为fact
 * sink：clickhouse.fact
 *
 */
public class LoadUser {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		//本地调试UI
		if("dev".equalsIgnoreCase(Util.getConfig().get("common.mode").toString())) {
			Configuration conf = new Configuration();
			conf.setString("rest.bind-port", "8081");
		    env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		}
		
		env.getConfig().setGlobalJobParameters(
			ParameterTool.fromPropertiesFile(Util.class.getClassLoader().getResourceAsStream("ilife.properties"))
		);
		
		// 1.1 把kafka设置为source
//		env.enableCheckpointing(5000); // checkpoint every 5000 msecs
		KafkaSource<String> json = KafkaSource.<String>builder()
			.setBootstrapServers(Util.getConfig().getProperty("brokers"))
			.setTopics("user")
			.setGroupId("flink-fact")
			.setStartingOffsets(OffsetsInitializer.earliest())
			.setValueOnlyDeserializer(new SimpleStringSchema())
			//.setDeserializer(KafkaRecordDeserializationSchema.of(new JsonDeserialization(true, true)))
			.build();
		DataStreamSource<String> source = env.fromSource(json, WatermarkStrategy.noWatermarks(), "receive-user-json");
		
		DataStream<String> facts = source
			.process(new UserJsonCsvParser())
			.name("parse-user-json-to-csv");
		
		//clickhouse sink
		Properties props = Util.getConfig();
		props.put("socket_timeout", Util.getConfig().getProperty("clickhouse.sink.timeout-sec"));//重要：缺少socket_timeout会导致clickhouse连接超时
		
//		props.put(ClickHouseSinkConst.TIMEOUT_SEC, Util.getConfig().getProperty("clickhouse.sink.timeout-sec"));//重要：缺少socket_timeout会导致clickhouse连接超时
//		props.put(ClickHouseSinkConst.MAX_BUFFER_SIZE, Util.getConfig().getProperty("clickhouse.sink.max-buffer-size"));
//		props.put(ClickHouseSinkConst.NUM_RETRIES, Util.getConfig().getProperty("clickhouse.sink.retries"));
//		props.put(ClickHouseSinkConst.NUM_WRITERS, Util.getConfig().getProperty("clickhouse.sink.num-writers"));
//		props.put(ClickHouseSinkConst.QUEUE_MAX_CAPACITY, Util.getConfig().getProperty("clickhouse.sink.queue-max-capacity"));
//		props.put(ClickHouseSinkConst.FAILED_RECORDS_PATH, Util.getConfig().getProperty("clickhouse.sink.failed-records-path"));
//		props.put(ClickHouseSinkConst.IGNORING_CLICKHOUSE_SENDING_EXCEPTION_ENABLED, Util.getConfig().getProperty("clickhouse.sink.ignoring-clickhouse-sending-exception-enabled"));
		
		if("dev".equalsIgnoreCase(Util.getConfig().get("common.mode").toString())) {
			props.put(ClickHouseSinkConst.MAX_BUFFER_SIZE, "10");//本地调试小批量写入查看结果
			props.put("socket_timeout", 60000);//重要：缺少socket_timeout会导致clickhouse连接超时
		}
		ClickHouseSink sink = new ClickHouseSink(props);
		
		facts.addSink(sink).name("insert-clickhouse");
		
		if("dev".equalsIgnoreCase(Util.getConfig().get("common.mode").toString())) {
			facts.print().name("print-console");
		}

		// execute program
		env.execute("load-user");
	}
}


