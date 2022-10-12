/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.playgrounds.ops.clickcount;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.playgrounds.ops.clickcount.functions.BackpressureMap;
import org.apache.flink.playgrounds.ops.clickcount.functions.ClickEventStatisticsCollector;
import org.apache.flink.playgrounds.ops.clickcount.functions.CountingAggregator;
import org.apache.flink.playgrounds.ops.clickcount.functions.DimAsyncFunction;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEvent;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEventStatistics;
import org.apache.flink.playgrounds.ops.clickcount.sink.KafkaMockSink;
import org.apache.flink.playgrounds.ops.clickcount.source.KafkaMockSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.TimeUnit;

/**
 * A simple streaming job reading {@link ClickEvent}s from Kafka, counting events per 15 seconds and
 * writing the resulting {@link ClickEventStatistics} back to Kafka.
 *
 * <p> It can be run with or without checkpointing and with event time or processing time semantics.
 * </p>
 *
 * <p>The Job can be configured via the command line:</p>
 * * "--checkpointing": enables checkpointing
 * * "--checkpoint.interval.mills": checkpoint interval
 * * "--event-time": set the StreamTimeCharacteristic to EventTime
 * * "--backpressure": insert an operator that causes periodic backpressure
 * * "--chaining": enable operator chain
 * * "--backpressure.block.mills": mock backpressure block time
 *
 * 功能：从模拟数据源输入数据，然后进行窗口统计输出。
 * 注：只测试背压，算子链相关事项，如果要测试exactly-once，请使用 ClickEventGenerator 生成数据，ClickEventCount 计算
 * 1.线上运行，相关配置只能配在flink部署目录中
 * metrics.reporter.promgateway.xxx
 */
public class MyClickEventCount {

	public static final String CHECKPOINTING_OPTION = "checkpointing";
	public static final String CHECKPOINT_INTERVAL_MILLS = "checkpoint.interval.mills";
	public static final String EVENT_TIME_OPTION = "event-time";
	public static final String BACKPRESSURE_OPTION = "backpressure";
	public static final String OPERATOR_CHAINING_OPTION = "chaining";
	public static final String BACKPRESSURE_BLOCK_MILLS = "backpressure.block.mills";

	public static final Time WINDOW_SIZE = Time.of(15, TimeUnit.SECONDS);

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		Configuration configuration = initPrometheusConfig();
		// 线上部署从部署目录提取conf/flink-conf.yaml配置予以改造，线下idea调试，则直接进行基于内存创建 或 读取resources/flink-conf.yaml 当初本地部署配置文件使用

		// flink部署目录conf/flink-conf.yaml中配置的是默认的名称，为更好定位通常需要定制化名称
		configuration.setString("metrics.reporter.promgateway.jobName","MyClickEventCount");
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

		configureEnvironment(params, env);

		boolean inflictBackpressure = params.has(BACKPRESSURE_OPTION);
		long blockMills = params.getLong(BACKPRESSURE_BLOCK_MILLS, 100);

		DataStream<ClickEvent> clicks = env.addSource(new KafkaMockSource())
			.name("ClickEvent Source")
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ClickEvent>(Time.of(200, TimeUnit.MILLISECONDS)) {
				@Override
				public long extractTimestamp(final ClickEvent element) {
					return element.getTimestamp().getTime();
				}
			});

		if (inflictBackpressure) {
			// Force a network shuffle so that the backpressure will affect the buffer pools
			clicks = clicks
				.keyBy(ClickEvent::getPage)
				.map(new BackpressureMap(blockMills))
				.name("Backpressure");
		}

		DataStream<ClickEvent> expandedClicks = AsyncDataStream.unorderedWait(clicks,new DimAsyncFunction(),200L,TimeUnit.MILLISECONDS)
				.name("join-cache");

		DataStream<ClickEventStatistics> statistics = expandedClicks
			.keyBy(ClickEvent::getPage)
			.timeWindow(WINDOW_SIZE)
			.aggregate(new CountingAggregator(),
				new ClickEventStatisticsCollector())
			.name("ClickEvent Counter");

		statistics.addSink(new KafkaMockSink())
				.name("ClickEventStatistics Sink");

		env.execute("My Click Event Count");
	}

	private static void configureEnvironment(
			final ParameterTool params,
			final StreamExecutionEnvironment env) {

		boolean checkpointingEnabled = params.has(CHECKPOINTING_OPTION);
		boolean eventTimeSemantics = params.has(EVENT_TIME_OPTION);
		boolean enableChaining = params.has(OPERATOR_CHAINING_OPTION);

		if (checkpointingEnabled) {
			long ckpInterval = params.getLong(CHECKPOINT_INTERVAL_MILLS, 1000);
			env.enableCheckpointing(ckpInterval);
		}

		if (eventTimeSemantics) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		}

		if(!enableChaining){
			//disabling Operator chaining to make it easier to follow the Job in the WebUI
			env.disableOperatorChaining();
		}
	}

	/**
	 * ##### 与Prometheus集成配置 #####
	 * metrics.reporter.promgateway.class: org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter
	 * # PushGateway的主机名与端口号
	 * metrics.reporter.promgateway.host: hadoop01
	 * metrics.reporter.promgateway.port: 9091
	 * # Flink metric在前端展示的标签（前缀）与随机后缀
	 * metrics.reporter.promgateway.jobName: flink-metrics
	 * metrics.reporter.promgateway.randomJobNameSuffix: true
	 * metrics.reporter.promgateway.deleteOnShutdown: true
	 * metrics.reporter.promgateway.interval: 30 SECONDS
	 */
	private static Configuration initPrometheusConfig(){
		String property = System.getProperty("os.name");
		Configuration configuration = null;
		// 现启动的 flink集群，比如 yarn-pre-job提交、idea本地运行现启动的standalone集群，可以通过resource/flink-conf.yaml 或 new Configuration() 方式声明配置
		// 如果是已经启动好的集群，比如standalone部署的集群、yarn-session提交，启动时刻flink-conf.yaml已经生效，无法通过本地配置文件，或new方式修改

		// 如果是idea调试，下面操作会指定定位到本地编译后的flink-conf.yaml文件，然后进行加载，
		//或者直接new Configuration()，然后调用 initPrometheusConfig 直接初始化，相当于在本地内存创建了一个 flink-conf.yaml
		// String resource = Thread.currentThread().getContextClassLoader().getResource("").getPath();
		// Configuration configuration = GlobalConfiguration.loadConfiguration(resource);
		if(property.equals("Mac OS X")){
			// 本地idea调试
			// 方法1：直接 new Configuration，然后手动创建基于内存的flink-conf.yaml
			 configuration = GlobalConfiguration.loadConfiguration();
             configuration.setString("metrics.reporter.promgateway.class","org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter");
             configuration.setString("metrics.reporter.promgateway.host","hadoop01");
             configuration.setString("metrics.reporter.promgateway.port","9091");
             configuration.setString("metrics.reporter.promgateway.jobName","flink-metrics");
             configuration.setString("metrics.reporter.promgateway.randomJobNameSuffix","true");
             configuration.setString("metrics.reporter.promgateway.deleteOnShutdown","false");
             configuration.setString("metrics.reporter.promgateway.interval","30 SECONDS");

			// 方法2：直接定位到本地编译后，从resources目录拷贝到target目录的flink-conf.yaml文件
			// String resource = Thread.currentThread().getContextClassLoader().getResource("").getPath();
			// configuration = GlobalConfiguration.loadConfiguration(resource);
		}else{
			// 如果是线上部署，下面的new 会直接定位到flink部署目录的配置文件
			configuration = GlobalConfiguration.loadConfiguration();
		}
		return configuration;
	}

}
