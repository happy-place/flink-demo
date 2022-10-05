package com.bigdata.flink.proj.testing.taxi.scala

import com.alibaba.fastjson.JSONObject
import com.bigdata.flink.proj.common.func.CommonUtil.{getClassPath, getResourcePath, runAsyncJob, sendMessage}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}

import java.lang.{Integer => JInt}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, StreamTableEnvironment, dataSetConversions, dataStreamConversions, tableConversions}
import org.apache.flink.table.api._
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableAggregateFunction, TableFunction}
import org.apache.flink.api.common.functions.{AggregateFunction => CAggregateFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{CheckpointConfig, ExecutionCheckpointingOptions}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.table.sinks.CsvTableSink.CsvFormatter
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.Collector
import org.junit.Test

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{Duration, ZoneId}
import java.util
import java.util.Collections
import scala.collection.mutable.ListBuffer
import collection.mutable._


/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/26 9:19 上午 
 * @desc:
 *
 */
class SqlTest {

  @Test
  def test1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val tab = env.fromElements(
      ("a1","b1",100),
      ("a1","b2",300),
      ("a2","b1",200),
      ("a2","b2",100),
    ).toTable(tabEnv,'user,'product,'amount)

    // 查询未注册表
    // executeSql() 返回值是 TableResult，可以直接打印输出；sqlQuery()返回值是 Table，需要 execute() 才能输出
    tabEnv.sqlQuery(s"select user,sum(amount) as sum_amount from $tab group by user")
      .execute()
      .print()

    tabEnv.createTemporaryView("tab",tab)

    // 查询已注册表
    tabEnv.sqlQuery("select user,sum(amount) as sum_amount from tab group by user")
      .execute()
      .print()

    val schema = new Schema()
      .field("product",DataTypes.STRING())
      .field("amount",DataTypes.INT())

    val output = getResourcePath("output/prod.csv")
    println(output)
    tabEnv.connect(new FileSystem().path(output))
      .withFormat(new Csv().fieldDelimiter(',').deriveSchema())
      .withSchema(schema)
      .inAppendMode()
      .createTemporaryTable("RubberOrders")

    // 有 await 才能输出
    tabEnv.executeSql("insert into RubberOrders select product,amount from tab where user like 'a1%'").await()

  }

  @Test
  def tableResult(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val config = tabEnv.getConfig.getConfiguration
    config.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE) // checkpoint 方式：精准一次性消费
    config.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,Duration.ofSeconds(60)) // checkpoint 间隔，本次结束到下次开始间隔
    config.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT,Duration.ofSeconds(120)) // checkpoint 超时丢弃

    val tab = env.fromElements(
      ("a1","b1",100),
      ("a1","b2",300),
      ("a2","b1",200),
      ("a2","b2",100),
    ).toTable(tabEnv,'user,'product,'amount)

    val result = tabEnv.executeSql(s"select * from $tab")
    val iter = result.collect() // tableResult 执行 collect 获得迭代器，或直接 print 输出
    try{
      while(iter.hasNext){
        println(iter.next())
      }
    }finally {
      iter.close()
    }
  }

  @Test
  def groupOver(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val config = tabEnv.getConfig.getConfiguration
    config.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE)
    config.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,Duration.ofSeconds(60))
    config.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT,Duration.ofSeconds(120))

    val tab = env.fromElements(
      ("a1","b1",100,new Timestamp(1)),
      ("a1","b2",200,new Timestamp(1)),
      ("a1","b3",300,new Timestamp(1)),
      ("a1","b4",400,new Timestamp(1)),
      ("a1","b5",500,new Timestamp(1)),
    ).toTable(tabEnv,'user,'product,'amount)

    tabEnv.createTemporaryView("tab",tab)

    tabEnv.sqlQuery("select user,sum(amount) as sum_amount from tab group by user")

  }


}
