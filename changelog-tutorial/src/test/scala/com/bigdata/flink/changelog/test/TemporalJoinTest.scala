package com.bigdata.flink.changelog.test

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.UnresolvedFieldExpression
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, dataStreamConversions, _}
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.junit.Test

import java.text.SimpleDateFormat
import java.time.ZoneId

class TemporalJoinTest {

  @Test
  def temporalTableFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    // 订单
    val orders = env.fromElements((2L, "Euro", "2021-05-11 12:00:02"),
      (1L, "US Dollar", "2021-05-11 12:00:02"),
      (50L, "Yen", "2021-05-11 12:00:04"),
        (3L, "Euro", "2021-05-11 12:00:05"),
    ).map{t =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (t._1,t._2,sdf.parse(t._3).getTime)
    }.assignTimestampsAndWatermarks(new WatermarkTimestampExtractor[Long,String]())
      .toTable(tabEnv,'amount,'currency,'rowtime.rowtime())

    tabEnv.createTemporaryView("Orders",orders)

    // 汇率
    val rateHistory = env.fromElements(
      ("US Dollar", 102L, "2021-05-11 12:00:01"),
      ("Euro", 114L, "2021-05-11 12:00:01"),
      ("Yen", 1L, "2021-05-11 12:00:01"),
      ("Euro", 116L, "2021-05-11 12:00:05"),
      ("Euro", 119L, "2021-05-11 12:00:07"),
    ).map{t =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (t._1,t._2,sdf.parse(t._3).getTime)
    }.assignTimestampsAndWatermarks(new WatermarkTimestampExtractor[String, Long]())
      .toTable(tabEnv, 'currency, 'rate, 'rowtime.rowtime())

    tabEnv.createTemporaryView("RateHistory",rateHistory)

    val rates = rateHistory.createTemporalTableFunction('rowtime, 'currency) // 声明时间字段和主键
    tabEnv.createTemporaryFunction("Rates",rates)

    tabEnv.sqlQuery(
      """
        |SELECT
        | o.currency
        | ,o.amount
        | ,r.rate
        | ,o.amount * r.rate as yen_amount
        | ,o.rowtime as o_time
        | ,r.rowtime as r_time
        |FROM Orders o,
        | LATERAL TABLE (Rates(o.rowtime)) as r
        |WHERE o.currency = r.currency
        |""".stripMargin
    ).execute()
      .print()

  }


  @Test
  def temporalTableJoin(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    // 订单
    val orders = env.fromElements((2L, "Euro", "2021-05-11 12:00:02"),
      (1L, "US Dollar", "2021-05-11 12:00:02"),
      (50L, "Yen", "2021-05-11 12:00:04"),
      (3L, "Euro", "2021-05-11 12:00:05"),
    ).map{t =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (t._1,t._2,sdf.parse(t._3).getTime)
    }.assignTimestampsAndWatermarks(new WatermarkTimestampExtractor[Long,String]())
      .toTable(tabEnv,'amount,'currency,'rowtime.rowtime())

    tabEnv.createTemporaryView("Orders",orders)

    tabEnv.executeSql(
      """
        |CREATE TABLE RateHistory (
        | currency STRING,
        | rate BIGINT,
        | rowtime TIMESTAMP(3),
        | PRIMARY KEY(currency) NOT ENFORCED
        |) with (
        |  'connector' = 'jdbc',
        |  'url' = 'jdbc:mysql://hadoop01:3306/test?serverTimezone=Asia/Shanghai',
        |  'driver' = 'com.mysql.cj.jdbc.Driver',
        |  'table-name' = 'rates',
        |  'username' = 'test',
        |  'password' = 'test'
        |)
        |""".stripMargin)

    tabEnv.sqlQuery(
      """
        |SELECT
        | o.currency
        | ,o.amount
        | ,r.rate
        | ,o.amount * r.rate as yen_amount
        | ,o.rowtime as o_time
        | ,r.rowtime as r_time
        |FROM Orders o
        |LEFT JOIN RateHistory FOR SYSTEM_TIME AS OF o.rowtime AS r
        |ON o.currency = r.currency
        |""".stripMargin
    ).execute()
      .print()

  }

}

class WatermarkTimestampExtractor[T1,T2] extends BoundedOutOfOrdernessTimestampExtractor[(T1,T2,Long)](Time.seconds(10)) {
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  override def extractTimestamp(t: (T1, T2, Long)): Long = t._3
}