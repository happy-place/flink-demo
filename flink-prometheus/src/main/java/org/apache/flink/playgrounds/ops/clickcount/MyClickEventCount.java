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
import org.apache.flink.playgrounds.ops.clickcount.functions.BackpressureMap;
import org.apache.flink.playgrounds.ops.clickcount.functions.ClickEventStatisticsCollector;
import org.apache.flink.playgrounds.ops.clickcount.functions.CountingAggregator;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEvent;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEventDeserializationSchema;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEventStatistics;
import org.apache.flink.playgrounds.ops.clickcount.records.ClickEventStatisticsSerializationSchema;
import org.apache.flink.playgrounds.ops.clickcount.sink.KafkaMockSink;
import org.apache.flink.playgrounds.ops.clickcount.source.KafkaMockSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
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

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

		DataStream<ClickEventStatistics> statistics = clicks
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
}
