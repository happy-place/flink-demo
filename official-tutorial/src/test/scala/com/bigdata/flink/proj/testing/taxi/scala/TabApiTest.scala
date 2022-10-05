package com.bigdata.flink.proj.testing.taxi.scala

import com.alibaba.fastjson.JSONObject
import com.bigdata.flink.proj.common.func.CommonUtil.{runAsyncJob, sendMessage}
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
import org.apache.flink.types.{Row, RowKind}
import org.apache.flink.util.Collector
import org.junit.Test

import java.text.SimpleDateFormat
import java.time.{Duration, ZoneId}
import java.util
import java.util.Collections
import scala.collection.mutable.ListBuffer
import collection.mutable._

/**
 * 函数式调用table api
 *
 */
class TabApiTest {

  // 无窗口
  @Test
  def test1(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchEnv = BatchTableEnvironment.create(env)
//    TableEnvironment.create(env) // 传入ExecutionConfig
//    StreamTableEnvironment.create(env) // 传入StreamExecutionEnv
    val orders = env.fromElements(
      ("a1", "b1", "c1"),
      ("a1", "b2", "c1"),
      ("a2", "b1", "c2"),
    ).toTable(batchEnv, 'a, 'b, 'c)

    orders.groupBy('a)
      .select('a,'b.count as 'cnt)
      .toDataSet[Row]
      .print()

  }

  /**
   * +----+--------------------------------+--------------------------------+-------------------------+
   * | op |                         amount |                       currency |                   ctime |
   * +----+--------------------------------+--------------------------------+-------------------------+
   * | +I |                              1 |                      US Dollar |     2021-05-21T12:00:02 |
   * | +I |                              8 |                           Euro |     2021-05-21T11:45:02 |
   * | +I |                              2 |                           Euro |     2021-05-21T12:00:02 |
   * | +I |                             50 |                            Yen |     2021-05-21T12:00:04 |
   * | +I |                              3 |                           Euro |     2021-05-21T12:00:05 |
   */
  @Test
  def zoneId(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
    // setLocalTimeZone 对 ddl 建表语句适用，对于 datastream -> toTable 不适用

    val messages = List(
      (8L, "Euro", "2021-05-21 11:45:02"),
      (2L, "Euro", "2021-05-21 12:00:02"),
      (1L, "US Dollar", "2021-05-21 12:00:02"),
      (50L, "Yen", "2021-05-21 12:00:04"),
      (3L, "Euro", "2021-05-21 12:00:05"),
    ).map{t =>
      val obj = new JSONObject()
      obj.put("amount",t._1)
      obj.put("currency",t._2)
      obj.put("ctime",t._3)
      obj
    }

    runAsyncJob(sendMessage("orders",messages,"currency"),5)

    tabEnv.executeSql(
      """
        |CREATE TABLE orders (
        |   amount STRING,
        |   currency STRING,
        |   ctime TIMESTAMP(3),
        |   WATERMARK FOR ctime AS ctime - INTERVAL '5' SECONDS
        |) WITH (
        | 'connector' = 'kafka',
        | 'topic' = 'orders',
        | 'scan.startup.mode' = 'latest-offset',
        | 'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
        | 'value.format' = 'json'
        |)
        |""".stripMargin)

    tabEnv.sqlQuery("select * from orders")
      .execute()
      .print()


  }

  // batch env 是纯粹 MR思路，用的时间就是
  @Test
  def test2(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchEnv = BatchTableEnvironment.create(env)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    batchEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
    // 尽管设置了 时区，但还是没有用，只能手动+8*3600*1000 到时间戳上
    val orders = env.fromElements(
      ("a1",11,null,"2021-05-11 01:01:00"),
      ("A1",12,"c1","2021-05-11 01:30:00"),
      ("a1",12,"c2","2021-05-11 02:01:00"),
      ("a1",30,"c3","2021-05-11 03:01:00"),
    ).map{t =>
      (t._1,t._2,t._3,sdf.parse(t._4).getTime)
    }.toTable(batchEnv,'a,'b,'c,'rowtime)
    // rowtime and .proctime time indicators are not allowed in a batch environment.

    orders.filter('a.isNotNull && 'b.isNotNull && 'c.isNotNull)
      .select('a.upperCase() as 'a,$"b",'c,$"rowtime")
      .window(Tumble over 1.hour() on 'rowtime as 'hourlyWindow)
      .groupBy('hourlyWindow,'a)
      .select(
        'hourlyWindow.start() as 'hourlyWindowStart,
        'hourlyWindow.end() as 'hourlyWindowEnd,
        'a,
        'c.count() as 'cnt,
        'b.avg() as 'avg_b
      ).toDataSet[Row]
      .print()
  }

  @Test
  def test3(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchEnv = BatchTableEnvironment.create(env)

    val rows = List(row("Bob", 12.32,30),
      row("Alice", 20.31,34),
      row("Bob", 12.00,99))

    batchEnv.fromValues(
      rows:_*
    ).printSchema()
    /*
      root
       |-- f0: VARCHAR(5) NOT NULL
       |-- f1: DOUBLE NOT NULL
       |-- f2: INT NOT NULL
     */

    val tab2 = batchEnv.fromValues(DataTypes.ROW(
      DataTypes.FIELD("name",DataTypes.STRING().nullable()),
      DataTypes.FIELD("amount",DataTypes.DECIMAL(12,2)), // 总位数 12 ，小数点之后位数2
      DataTypes.FIELD("rate",DataTypes.INT().bridgedTo(classOf[java.lang.Integer])), //
    ),rows:_*)
    tab2.printSchema()

    /*
      root
       |-- name: STRING  // 允许为空
       |-- amount: DECIMAL(12, 2)
       |-- rate: INT  // 声明使用 scala.Int
     */

    batchEnv.createTemporaryView("tab2",tab2) // 注册到 flink 内置catalog

    batchEnv.from("tab2")
      .as('n,'a,'r)
      .select('*)
      .select($"n" as 'name,$"a" as 'amount,'r as 'rate)
      .filter('rate > 50) // filter("r > 50")
      .where('name=== "Bob")
      .execute()
      .print()

    /**
     * +--------------------------------+----------------+-------------+
     * |                           name |         amount |        rate |
     * +--------------------------------+----------------+-------------+
     * |                            Bob |           12.0 |          99 |
     * +--------------------------------+----------------+-------------+
     */

    batchEnv.from("tab2")
      .addColumns('amount * 'rate as 'total) // 添加新列
      .execute()
      .print()

    /**
     * +--------------------------------+----------------+-------------+--------------------------+
     * |                           name |         amount |        rate |                    total |
     * +--------------------------------+----------------+-------------+--------------------------+
     * |                            Bob |           12.0 |          99 |                   1188.0 |
     * |                          Alice |          20.31 |          34 |                   690.54 |
     * |                            Bob |          12.32 |          30 |                   369.60 |
     * +--------------------------------+----------------+-------------+--------------------------+
     */

    batchEnv.from("tab2")
      .addOrReplaceColumns('amount * 'rate as 'amount) // 新列替换老列
      .execute()
      .print()

    /**
     * +--------------------------------+--------------------------+-------------+
     * |                           name |                   amount |        rate |
     * +--------------------------------+--------------------------+-------------+
     * |                            Bob |                   369.60 |          30 |
     * |                            Bob |                   1188.0 |          99 |
     * |                          Alice |                   690.54 |          34 |
     * +--------------------------------+--------------------------+-------------+
     */

    batchEnv.from("tab2")
      .dropColumns('rate)
      .execute()
      .print()

    /**
     * +--------------------------------+----------------+
     * |                           name |         amount |
     * +--------------------------------+----------------+
     * |                          Alice |          20.31 |
     * |                            Bob |           12.0 |
     * |                            Bob |          12.32 |
     * +--------------------------------+----------------+
     */

    batchEnv.from("tab2")
      .renameColumns("rate as rate1") // 重命名列
      .execute()
      .print()

    /**
     * +--------------------------------+----------------+-------------+
     * |                           name |         amount |       rate1 |
     * +--------------------------------+----------------+-------------+
     * |                          Alice |          20.31 |          34 |
     * |                            Bob |          12.32 |          30 |
     * |                            Bob |           12.0 |          99 |
     * +--------------------------------+----------------+-------------+
     */

  }

  // stream batch都可以使用
  @Test
  def groupBy(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchEnv = BatchTableEnvironment.create(env)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    batchEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
    // 尽管设置了 时区，但还是没有用，只能手动+8*3600*1000 到时间戳上
    val orders = env.fromElements(
      ("a1",11,null,"2021-05-11 01:01:00"),
      ("A1",12,"c1","2021-05-11 01:30:00"),
      ("a1",12,"c2","2021-05-11 02:01:00"),
      ("a1",30,"c3","2021-05-11 03:01:00"),
    ).map{t =>
      (t._1,t._2,t._3,sdf.parse(t._4).getTime)
    }.toTable(batchEnv,'a,'b,'c,'rowtime) // batchEnv 不能设置时间语义

    // 无时间group by
    orders.groupBy('a)
      .select('a,'b.sum() as 'b_sum)
      .execute()
      .print()

    /**
     * +--------------------------------+-------------+
     * |                              a |       b_sum |
     * +--------------------------------+-------------+
     * |                             a1 |          53 |
     * |                             A1 |          12 |
     * +--------------------------------+-------------+
     */

    // 基于窗口统计
    orders.window( Tumble over 1.hours() on 'rowtime as 'w )
      .groupBy('w,'a)
      .select('w.start() as 'w_start,'w.end() as 'w_end,'w.rowtime() as 'real_end,'a,'b.sum() as 'b_sum) //
      .execute()
      .print()

    /** 'w.rowtime() 窗口真正结束时间
     * +-------------------------+-------------------------+-------------------------+--------------------------------+-------------+
     * |                 w_start |                   w_end |                real_end |                              a |       b_sum |
     * +-------------------------+-------------------------+-------------------------+--------------------------------+-------------+
     * |   2021-05-10 17:00:00.0 |   2021-05-10 18:00:00.0 | 2021-05-10 17:59:59.999 |                             a1 |          11 |
     * |   2021-05-10 19:00:00.0 |   2021-05-10 20:00:00.0 | 2021-05-10 19:59:59.999 |                             a1 |          30 |
     * |   2021-05-10 17:00:00.0 |   2021-05-10 18:00:00.0 | 2021-05-10 17:59:59.999 |                             A1 |          12 |
     * |   2021-05-10 18:00:00.0 |   2021-05-10 19:00:00.0 | 2021-05-10 18:59:59.999 |                             a1 |          12 |
     * +-------------------------+-------------------------+-------------------------+--------------------------------+-------------+
     */

  }


  // 只有stream 使用
  @Test
  def overWindow(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val batchEnv = StreamTableEnvironment.create(env)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    batchEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
    // 尽管设置了 时区，但还是没有用，只能手动+8*3600*1000 到时间戳上
    val orders = env.fromElements(
      ("a1",20,null,"2021-05-11 01:01:00"),
      ("a1",12,"c1","2021-05-11 01:30:00"),
      ("a1",30,"c2","2021-05-11 02:01:00"),
      ("a1",10,"c3","2021-05-11 03:01:00"),
    ).map{t =>
      (t._1,t._2,t._3,sdf.parse(t._4).getTime)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Int,String,Long)]() {
        override def extractTimestamp(t: (String, Int, String, Long), l: Long): Long = t._4
      })
    ).toTable(batchEnv,'a,'b,'c,'rowtime.rowtime())

    // over window 从指定位置圈一定数据；例如从起始行到当前行，必须指定时间字段，因此要使用StreamExecutionEnv
    // UNBOUNDED_ROW、UNBOUNDED_RANGE 窗口无上届，能抓的都抓过来 row 只count记录数，range 指时间
    // CURRENT_ROW CURRENT_RANGE 到当前行
    orders.window(
      Over partitionBy 'a
        orderBy'rowtime
        preceding UNBOUNDED_ROW
        following CURRENT_ROW
        as 'w
    ).select('a,'b.avg.over('w) as 'b_avg,'b.min.over('w) as 'b_min)
      .execute()
      .print()

    /**
     * +----+--------------------------------+-------------+-------------+
     * | op |                              a |       b_avg |       b_min |
     * +----+--------------------------------+-------------+-------------+
     * | +I |                             a1 |          20 |          20 |
     * | +I |                             a1 |          16 |          12 |
     * | +I |                             a1 |          20 |          12 |
     * | +I |                             a1 |          18 |          10 |
     * +----+--------------------------------+-------------+-------------+
     */
  }

  @Test
  def distinct1(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchEnv = BatchTableEnvironment.create(env)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    batchEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
    // 尽管设置了 时区，但还是没有用，只能手动+8*3600*1000 到时间戳上
    val orders = env.fromElements(
      ("a1",11,null,"2021-05-11 01:01:00"),
      ("a1",11,"c1","2021-05-11 01:30:00"),
      ("a1",12,"c2","2021-05-11 02:01:00"),
      ("a1",30,"c3","2021-05-11 03:01:00"),
    ).map{t =>
      (t._1,t._2,t._3,sdf.parse(t._4).getTime)
    }.toTable(batchEnv,'a,'b,'c,'rowtime)

    orders.groupBy('a)
      .select('a,'b.sum.distinct() as 'd) // sum(distinct b) 去重后求和
      .execute()
      .print()

    /**
     * +--------------------------------+-------------+
     * |                              a |           d |
     * +--------------------------------+-------------+
     * |                             a1 |          53 |
     * +--------------------------------+-------------+
     */

    orders.window(Tumble over 1.hours() on 'rowtime as 'w)
      .groupBy('w,'a)
      .select('w.start() as 'w_start,'w.end() as 'w_end,'a,'b.sum().distinct() as 'd)
      .execute()
      .print()
  }

  @Test
  def distinct2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val batchEnv = StreamTableEnvironment.create(env)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    batchEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
    // 尽管设置了 时区，但还是没有用，只能手动+8*3600*1000 到时间戳上
    val orders = env.fromElements(
      ("a1",11,null,"2021-05-11 01:01:00"),
      ("a1",11,"c1","2021-05-11 01:30:00"),
      ("a1",30,"c2","2021-05-11 02:01:00"),
      ("a1",10,"c3","2021-05-11 03:01:00"),
    ).map{t =>
      (t._1,t._2,t._3,sdf.parse(t._4).getTime)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Int,String,Long)]() {
        override def extractTimestamp(t: (String, Int, String, Long), l: Long): Long = t._4
      })
    ).toTable(batchEnv,'a,'b,'c,'rowtime.rowtime())

    // StreamExecuteEnv 上对滚动窗口执行 distinct 聚合
    orders.window(Tumble over 1.hours() on 'rowtime as 'w)
      .groupBy('w,'a)
      .select('w.start() as 'w_start,'w.end() as 'w_end,'a,'b.sum().distinct() as 'd)
      .execute()
      .print()

    /**
     * +----+-------------------------+-------------------------+--------------------------------+-------------+
      | op |                 w_start |                   w_end |                              a |           d |
      +----+-------------------------+-------------------------+--------------------------------+-------------+
      | +I |        2021-05-10T17:00 |        2021-05-10T18:00 |                             a1 |          11 |
      | +I |        2021-05-10T18:00 |        2021-05-10T19:00 |                             a1 |          30 |
      | +I |        2021-05-10T19:00 |        2021-05-10T20:00 |                             a1 |          10 |
      +----+-------------------------+-------------------------+--------------------------------+-------------+
     */

  }

  @Test
  def distinct3(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val batchEnv = StreamTableEnvironment.create(env)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    batchEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))
    // 尽管设置了 时区，但还是没有用，只能手动+8*3600*1000 到时间戳上
    val orders = env.fromElements(
      ("a1",11,null,"2021-05-11 01:01:00"),
      ("a1",11,"c1","2021-05-11 01:30:00"),
      ("a1",30,"c2","2021-05-11 02:01:00"),
      ("a1",10,"c3","2021-05-11 03:01:00"),
    ).map{t =>
      (t._1,t._2,t._3,sdf.parse(t._4).getTime)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Int,String,Long)]() {
        override def extractTimestamp(t: (String, Int, String, Long), l: Long): Long = t._4
      })
    ).toTable(batchEnv,'a,'b,'c,'rowtime.rowtime())

    // StreamExecuteEnv 上对over 窗口执行 distinct 聚合
    orders.window(
      Over
      partitionBy 'a
      orderBy 'rowtime
      preceding UNBOUNDED_RANGE
      following CURRENT_RANGE
      as 'w
    ).select('a,'b.sum().distinct().over('w) as 'd)
      .execute()
      .print()

    /**
     * +----+--------------------------------+-------------+
     * | op |                              a |           d |
     * +----+--------------------------------+-------------+
     * | +I |                             a1 |          11 |
     * | +I |                             a1 |          11 |
     * | +I |                             a1 |          41 |
     * | +I |                             a1 |          51 |
     * +----+--------------------------------+-------------+
     */
  }

  @Test
  def distinct4(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val batchEnv = StreamTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    batchEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    // 不更新情况下，最长保留24h,最短保留12h，具体什么时间清理，取决于资源什么时候不够用
    batchEnv.getConfig.setIdleStateRetentionTime(Time.hours(12),Time.hours(24))
//    batchEnv.getConfig.setIdleStateRetention(Duration.ofHours(24)) // 空闲24h不更新就清空

    // 尽管设置了 时区，但还是没有用，只能手动+8*3600*1000 到时间戳上
    val orders = env.fromElements(
      ("a1",11,null,"2021-05-11 01:01:00"),
      ("a1",11,"c1","2021-05-11 01:30:00"),
      ("a1",30,"c2","2021-05-11 02:01:00"),
      ("a1",10,"c3","2021-05-11 03:01:00"),
    ).map{t =>
      (t._1,t._2,t._3,sdf.parse(t._4).getTime)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Int,String,Long)]() {
        override def extractTimestamp(t: (String, Int, String, Long), l: Long): Long = t._4
      })
    ).toTable(batchEnv,'a,'b,'c,'rowtime.rowtime())

    val mySum = new MySum()

    // 对自定义聚合函数使用distinct，先distinct 去重，然后 在执行自定义聚合逻辑
    // 由于去重需要保存明细，因此需要控制状态大小 batchEnv.getConfig.setIdleStateRetentionTime
    orders.window(
      Over
        partitionBy 'a
        orderBy 'rowtime
        preceding UNBOUNDED_RANGE
        following CURRENT_RANGE
        as 'w
    ).select('a,mySum.distinct('b).over('w) as 'd)
      .execute()
      .print()

    /**
     * +----+--------------------------------+-------------+
     * | op |                              a |           d |
     * +----+--------------------------------+-------------+
     * | +I |                             a1 |          11 |
     * | +I |                             a1 |          11 |
     * | +I |                             a1 |          41 |
     * | +I |                             a1 |          51 |
     * +----+--------------------------------+-------------+
     */

  }

  @Test
  def distinct5(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchEnv = BatchTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    batchEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    // 不更新情况下，最长保留24h,最短保留12h，具体什么时间清理，取决于资源什么时候不够用
    batchEnv.getConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24))
    //    batchEnv.getConfig.setIdleStateRetention(Duration.ofHours(24)) // 空闲24h不更新就清空

    // 尽管设置了 时区，但还是没有用，只能手动+8*3600*1000 到时间戳上
    val orders = env.fromElements(
      ("a1", 11, null, "2021-05-11 01:01:00"),
      ("a1", 11, null, "2021-05-11 01:01:00"),
      ("a1", 11, "c1", "2021-05-11 01:30:00"),
      ("a1", 30, "c2", "2021-05-11 02:01:00"),
      ("a1", 10, "c3", "2021-05-11 03:01:00"),
    ).map { t =>
      (t._1, t._2, t._3, sdf.parse(t._4).getTime)
    }.toTable(batchEnv, 'a, 'b, 'c, 'rowtime)

    // 整行去重 ,StreamExecutionEnv 下注意状态大小
    orders.distinct()
      .execute()
      .print()

    /**
     * +--------------------------------+-------------+--------------------------------+----------------------+
      |                              a |           b |                              c |              rowtime |
      +--------------------------------+-------------+--------------------------------+----------------------+
      |                             a1 |          10 |                             c3 |        1620673260000 |
      |                             a1 |          30 |                             c2 |        1620669660000 |
      |                             a1 |          11 |                             c1 |        1620667800000 |
      |                             a1 |          11 |                         (NULL) |        1620666060000 |
      +--------------------------------+-------------+--------------------------------+----------------------+
     */

  }

  @Test
  def join1(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchEnv = BatchTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val tab1 = env.fromElements(
      ("Bob",10,"2021-05-11 12:00:01"),
      ("Alice",12,"2021-05-11 12:30:01"),
      ("Bob",13,"2021-05-11 13:00:01"),
      ("Lily",14,"2021-05-11 13:20:01"),
      ("Tim",14,"2021-05-11 13:30:01"),
    ).map{t =>
      (t._1,t._2,sdf.parse(t._3).getTime + 8*3600*1000)
    }.toTable(batchEnv,'name1,'rate,'rowtime1)

    val tab2 = env.fromElements(
      ("Bob",10,"2021-05-11 12:00:01"),
      ("Lily",12,"2021-05-11 12:30:01"),
      ("Alice",13,"2021-05-11 13:00:01"),
      ("Lily",14,"2021-05-11 13:20:01"),
      ("Tom",14,"2021-05-11 13:30:01"),
    ).map{t =>
      (t._1,t._2,sdf.parse(t._3).getTime + 8*3600*1000)
    }.toTable(batchEnv,'name2,'amount,'rowtime2)

    // 无窗口 inner join
    // 先形成 cross join 然后通过where条件进行过滤
    tab1.join(tab2)
      .where('name1 === 'name2)
      .select('name1,'name2,'rate,'amount)
      .execute()
      .print()

    // 在join时，就根据条件进行连接，而不是基于where过滤
    tab1.join(tab2,'name1 === 'name2)
          .select('name1,'name2,'rate,'amount)
          .execute()
          .print()

    /**
     * +--------------------------------+--------------------------------+-------------+-------------+
     *  |                          name1 |                          name2 |        rate |      amount |
     *  +--------------------------------+--------------------------------+-------------+-------------+
     *  |                            Bob |                            Bob |          10 |          10 |
     *  |                            Bob |                            Bob |          13 |          10 |
     *  |                           Lily |                           Lily |          14 |          12 |
     *  |                           Lily |                           Lily |          14 |          14 |
     *  |                          Alice |                          Alice |          12 |          13 |
     *  +--------------------------------+--------------------------------+-------------+-------------+
     */

    // 名副其实 左外连接
    tab1.leftOuterJoin(tab2,'name1 === 'name2)
      .select('name1,'name2,'rate,'amount)
      .execute()
      .print()

    // 先借助 join 形成cross join，然通过where 过滤，尽管使用的是left join,但效果上仍是 inner join
//    tab1.leftOuterJoin(tab2)
//      .where('name1 === 'name2)
//      .select('name1,'name2,'rate,'amount)
//      .execute()
//      .print()

    /**
     * +--------------------------------+--------------------------------+-------------+-------------+
     * |                          name1 |                          name2 |        rate |      amount |
     * +--------------------------------+--------------------------------+-------------+-------------+
     * |                          Alice |                          Alice |          12 |          13 |
     * |                            Tim |                         (NULL) |          14 |      (NULL) |
     * |                            Bob |                            Bob |          10 |          10 |
     * |                            Bob |                            Bob |          13 |          10 |
     * |                           Lily |                           Lily |          14 |          12 |
     * |                           Lily |                           Lily |          14 |          14 |
     * +--------------------------------+--------------------------------+-------------+-------------+
     */

    // 全外连接
    tab1.fullOuterJoin(tab2,'name1 === 'name2)
      .select('name1,'name2,'rate,'amount)
      .execute()
      .print()

    /**
     * +--------------------------------+--------------------------------+-------------+-------------+
     * |                          name1 |                          name2 |        rate |      amount |
     * +--------------------------------+--------------------------------+-------------+-------------+
     * |                            Tim |                         (NULL) |          14 |      (NULL) |
     * |                         (NULL) |                            Tom |      (NULL) |          14 |
     * |                          Alice |                          Alice |          12 |          13 |
     * |                            Bob |                            Bob |          10 |          10 |
     * |                            Bob |                            Bob |          13 |          10 |
     * |                           Lily |                           Lily |          14 |          12 |
     * |                           Lily |                           Lily |          14 |          14 |
     * +--------------------------------+--------------------------------+-------------+-------------+
     */

  }

  @Test
  def join2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val tab1 = env.fromElements(
      ("Bob",10,"2021-05-11 12:00:01"),
      ("Alice",12,"2021-05-11 12:30:01"),
      ("Bob",13,"2021-05-11 13:00:01"),
      ("Lily",14,"2021-05-11 13:20:01"),
      ("Tim",14,"2021-05-11 13:30:01"),
    ).map{t =>
      (t._1,t._2,sdf.parse(t._3).getTime + 8*3600*1000)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String,Int,Long)] {
        override def extractTimestamp(t: (String, Int, Long), l: Long): Long = t._3
      })
    ).toTable(tabEnv,'name1,'rate,'rowtime1.rowtime())

    val tab2 = env.fromElements(
      ("Bob",10,"2021-05-11 12:00:01"),
      ("Lily",12,"2021-05-11 12:30:01"),
      ("Alice",13,"2021-05-11 13:00:01"),
      ("Lily",14,"2021-05-11 13:20:01"),
      ("Tom",14,"2021-05-11 13:30:01"),
    ).map{t =>
      (t._1,t._2,sdf.parse(t._3).getTime + 8*3600*1000)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String,Int,Long)] {
        override def extractTimestamp(t: (String, Int, Long), l: Long): Long = t._3
      })
    ).toTable(tabEnv,'name2,'amount,'rowtime2.rowtime())

    // tab2 是驱动表，tab1 是探测表，tab2的时间按interval窗口上下界，画三角形，圈住的tab1元素，就实现join,
    // tab2 画出的是时间范围，因此select中不能出现时间字段，而tab1中被圈住的是具体的元素，因此tab1的时间字段可以出现
    tab1.join(tab2).where('name1==='name2 &&
      'rowtime1 >= 'rowtime2 - 30.minutes() &&
      'rowtime1 < 'rowtime2 + 30.minutes()
    ).select('name1,'rowtime1,'rate,'amount)
      .execute()
      .print()

  }

  @Test
  def join3(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchEnv = BatchTableEnvironment.create(env)
    val tab = env.fromElements(
      ("a1",10,"java:98,python:20"),
      ("a2",20,"scala:99,python:50"),
      ("a3",20,null),
      ("a4",20,""),
    ).toTable(batchEnv,'id,'amount,'favors)

    val mySplit = new MySplit(",",":")

    // 将 favors 每一行 拆成一张表 (多行，且每行对应多列) UDTF 函数
    tab.joinLateral(mySplit('favors) as ('favor,'score))
      .execute()
      .print()

    /**
     * null 和 "" 都被忽略了
     * +----------+-------------+--------------------------------+--------------------------------+-------------+
      |       id |      amount |                         favors |                          favor |       score |
      +----------+-------------+--------------------------------+--------------------------------+-------------+
      |       a1 |          10 |              java:98,python:20 |                           java |          98 |
      |       a1 |          10 |              java:98,python:20 |                         python |          20 |
      |       a2 |          20 |             scala:99,python:50 |                          scala |          99 |
      |       a2 |          20 |             scala:99,python:50 |                         python |          50 |
      +----------+-------------+--------------------------------+--------------------------------+-------------+
     */

    tab.leftOuterJoinLateral(mySplit('favors) as ('favor,'score))
      .execute()
      .print()

    /** null、"" 都被保留了
     *+------------+-------------+--------------------------------+--------------------------------+-------------+
      |         id |      amount |                         favors |                          favor |       score |
      +------------+-------------+--------------------------------+--------------------------------+-------------+
      |         a1 |          10 |              java:98,python:20 |                           java |          98 |
      |         a1 |          10 |              java:98,python:20 |                         python |          20 |
      |         a2 |          20 |             scala:99,python:50 |                          scala |          99 |
      |         a2 |          20 |             scala:99,python:50 |                         python |          50 |
      |         a3 |          20 |                         (NULL) |                         (NULL) |      (NULL) |
      |         a4 |          20 |                                |                         (NULL) |      (NULL) |
      +------------+-------------+--------------------------------+--------------------------------+-------------+
     */
  }

  @Test
  def join4(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.getConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"))

    // 订单
    val orders = env.fromElements(
      (8L, "Euro", "2021-05-21 11:45:02"),
      (2L, "Euro", "2021-05-21 12:00:02"),
      (1L, "US Dollar", "2021-05-21 12:00:02"),
      (50L, "Yen", "2021-05-21 12:00:04"),
      (3L, "Euro", "2021-05-21 12:00:05"),
    ).map{t =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (t._1,t._2,sdf.parse(t._3).getTime + 8 * 3600 * 1000)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(Long,String,Long)] {
        override def extractTimestamp(t: (Long, String, Long), l: Long): Long = t._3
      })
    ).toTable(tabEnv,'amount,'o_currency,'o_rowtime.rowtime())

    tabEnv.executeSql(
      s"""
        |CREATE TABLE RatesHistory (
        |    currency STRING,
        |    rate INT,
        |    rowtime TIMESTAMP(3),
        |    PRIMARY KEY(currency) NOT ENFORCED,
        |    WATERMARK FOR rowtime as rowtime
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://hadoop01:3306/test?serverTimezone=Asia/Shanghai',
        |   'driver' = 'com.mysql.cj.jdbc.Driver',
        |   'table-name' = 'rates',
        |   'username' = 'test',
        |   'password' = 'test',
        |   'lookup.cache.max-rows' = '3000',
        |   'lookup.cache.ttl' = '10s',
        |   'lookup.max-retries' = '3'
        |)
        |""".stripMargin)

    val rateHis = tabEnv.from("RatesHistory")
    val rates = rateHis.createTemporalTableFunction('rowtime, 'currency)

    tabEnv.createTemporaryFunction("rates",rates)

    // VARCHAR(2147483647) CHARACTER SET "UTF-16LE" currency
    //  VARCHAR(2147483647) CHARACTER SET "UTF-16LE" NOT NULL currency
    orders.joinLateral(rates('o_rowtime),'o_currency === 'currency)
      .execute()
      .print()

//    tabEnv.sqlQuery(
//      """
//        |SELECT
//        | o_currency
//        | ,o.amount
//        | ,r.rate
//        | ,o.amount * r.rate as yen_amount
//        | ,o_rowtime as o_time
//        | ,r.rowtime as r_time
//        |FROM Orders o
//        |LEFT JOIN RatesHistory FOR SYSTEM_TIME AS OF o_rowtime as r
//        |ON o.o_currency = r.currency
//        |""".stripMargin
//    ).execute()
//      .print()

//    tabEnv.sqlQuery(
//      """
//        |SELECT
//        | o_currency
//        | ,o.amount
//        | ,r.rate
//        | ,o.amount * r.rate as yen_amount
//        | ,o_rowtime as o_time
//        | ,r.rowtime as r_time
//        |FROM Orders o,
//        | LATERAL TABLE (rates(o.o_rowtime)) as r
//        |WHERE o.o_currency = r.currency
//        |""".stripMargin
//    ).execute()
//      .print()

  }

  @Test
  def union(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchEnv = BatchTableEnvironment.create(env)
    val tab1 = env.fromElements(
      ("a1","b1",21),
      ("a1","b1",21),
      ("a2","b2",22),
      ("a2","b2",22),
    ).toTable(batchEnv,'a,'b,'c)

    val tab2 = env.fromElements(
      ("a1","b1",21),
      ("a1","b1",21),
      ("a3","b3",33),
    ).toTable(batchEnv,'a,'b,'c)

      // 去重
    tab1.union(tab2)
      .execute()
      .print()

    /**
     * +-------+----------------------+-------------+
     * |     a |                    b |           c |
     * +-------+----------------------+-------------+
     * |    a3 |                   b3 |          33 |
     * |    a2 |                   b2 |          22 |
     * |    a1 |                   b1 |          21 |
     * +-------+----------------------+-------------+
     */

    // unionAll 不去重
    tab1.unionAll(tab2)
      .execute()
      .print()

    /**
     * +--------------------------------+--------------------------------+-------------+
     * |                              a |                              b |           c |
     * +--------------------------------+--------------------------------+-------------+
     * |                             a2 |                             b2 |          22 |
     * |                             a3 |                             b3 |          33 |
     * |                             a1 |                             b1 |          21 |
     * |                             a1 |                             b1 |          21 |
     * +--------------------------------+--------------------------------+-------------+
     */

    // 交集，交集如果是同一个元素出现多次，则去重
    tab1.intersect(tab2)
      .execute()
      .print()

    /**
     * +--------------------------------+--------------------------------+-------------+
     * |                              a |                              b |           c |
     * +--------------------------------+--------------------------------+-------------+
     * |                             a1 |                             b1 |          21 |
     * +--------------------------------+--------------------------------+-------------+
     */

    // 交集，交集如果是同一个元素出现多次，则不去重
    tab1.intersectAll(tab2)
      .execute()
      .print()

    /**
     * +--------------------------------+--------------------------------+-------------+
     * |                              a |                              b |           c |
     * +--------------------------------+--------------------------------+-------------+
     * |                             a1 |                             b1 |          21 |
     * |                             a1 |                             b1 |          21 |
     * +--------------------------------+--------------------------------+-------------+
     */

    // tab1 对 tab2的差集 ，即出现在tab1但不出现在tab2，留下来的如果有重复执行去重
    tab1.minus(tab2)
      .execute()
      .print()

    /**
     *
     * +--------------------------------+--------------------------------+-------------+
     * |                              a |                              b |           c |
     * +--------------------------------+--------------------------------+-------------+
     * |                             a2 |                             b2 |          22 |
     * +--------------------------------+--------------------------------+-------------+
     */

    // tab1 对 tab2的差集 ，即出现在tab1但不出现在tab2，留下来的如果有重复，不执行去重
    tab1.minusAll(tab2)
      .execute()
      .print()

    /**
     * +--------------------------------+--------------------------------+-------------+
     * |                              a |                              b |           c |
     * +--------------------------------+--------------------------------+-------------+
     * |                             a2 |                             b2 |          22 |
     * |                             a2 |                             b2 |          22 |
     * +--------------------------------+--------------------------------+-------------+
     */

    // 等效于 intercetAll
    tab1.where('a in tab2.select('a))
      .execute()
      .print()

    /**
     * +--------------------------------+--------------------------------+-------------+
     * |                              a |                              b |           c |
     * +--------------------------------+--------------------------------+-------------+
     * |                             a1 |                             b1 |          21 |
     * |                             a1 |                             b1 |          21 |
     * +--------------------------------+--------------------------------+-------------+
     */
  }

  @Test
  def orderBy(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val batchEnv = BatchTableEnvironment.create(env)
    val tab1 = env.fromElements(
      ("a1","b1",21),
      ("a1","b1",21),
      ("a2","b2",22),
      ("a2","b2",22),
    ).toTable(batchEnv,'a,'b,'c)

    // 从第二位开始 top2
    tab1.orderBy('c desc)
      .offset(1)
      .limit(2)
      .execute()
      .print()

    // top2
    tab1.orderBy('c desc)
      .fetch(2)
      .execute()
      .print()
  }

  @Test
  def insert(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sittings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tabEnv = StreamTableEnvironment.create(env,sittings)
    val tab1 = env.fromElements(
      (100,10,"2021-05-11 11:00:00"),
      (101,11,"2021-05-11 12:00:00"),
    ).map{t =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (t._1,t._2,sdf.parse(t._3).getTime)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
      new SerializableTimestampAssigner[(Int,Int,Long)] {
        override def extractTimestamp(t: (Int, Int, Long), l: Long): Long = t._3
      }
    )).toTable(tabEnv,'currency,'rate,'rowtime.rowtime())

    tabEnv.createTemporaryView("tab1",tab1)

    tabEnv.executeSql(
      s"""
         |CREATE TABLE RatesHistory (
         |    currency INT,
         |    rate INT,
         |    rowtime TIMESTAMP(3)
         |) WITH (
         |   'connector' = 'jdbc',
         |   'url' = 'jdbc:mysql://hadoop01:3306/test?serverTimezone=Asia/Shanghai',
         |   'driver' = 'com.mysql.cj.jdbc.Driver',
         |   'table-name' = 'rates2',
         |   'username' = 'test',
         |   'password' = 'test',
         |   'sink.buffer-flush.interval' = '2s',
         |   'sink.buffer-flush.max-rows' = '300'
         |)
         |""".stripMargin)


    tabEnv.executeSql(
      """
        |CREATE TABLE RatesHistory2 (
        |    currency INT,
        |    rate INT,
        |    rowtime TIMESTAMP(3)
        | ) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'rates',
        |  'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
        |  'properties.group.id' = 'testGroup',
        |  'format' = 'json'
        | )
        |""".stripMargin)


//    batchEnv.executeSql("insert into RatesHistory(currency,rate,rowtime) select * from tab1")
//      .await() // sink 必须使用 await 或 print 才能触发提交

//    tab1.executeInsert("RatesHistory").await()

    tab1.executeInsert("RatesHistory2").await()

  }

  @Test
  def groupWindow(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sittings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tabEnv = StreamTableEnvironment.create(env,sittings)
    val tab1 = env.fromElements(
      (100,10,"2021-05-11 11:00:00"),
      (100,23,"2021-05-11 11:20:00"),
      (100,9,"2021-05-11 11:30:00"),
      (100,11,"2021-05-11 12:10:00"),
      (100,18,"2021-05-11 12:22:00"),
    ).map{t =>
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (t._1,t._2,sdf.parse(t._3).getTime)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
      new SerializableTimestampAssigner[(Int,Int,Long)] {
        override def extractTimestamp(t: (Int, Int, Long), l: Long): Long = t._3 + 8 * 3600 * 1000
      }
    )).toTable(tabEnv,'a,'b,'rowtime.rowtime())

    //  TumbleWindow、SessionWindow、SlideWindow 都是 GroupWindow 子类
    tab1.window(Tumble over 1.hours() on 'rowtime as 'w)
      .groupBy('w,'a)
      .select('w.start() as 'w_start,'w.end() as 'w_end,'a,'b.max() as 'max_b)
      .execute()
      .print()

    /**
     * +----+-------------------------+-------------------------+-------------+-------------+
      | op |                 w_start |                   w_end |           a |       max_b |
      +----+-------------------------+-------------------------+-------------+-------------+
      | +I |        2021-05-11T11:00 |        2021-05-11T12:00 |         100 |          23 |
      | +I |        2021-05-11T12:00 |        2021-05-11T13:00 |         100 |          18 |
      +----+-------------------------+-------------------------+-------------+-------------+
     */


    tab1.window(Slide over 1.hours() every 30.minutes() on 'rowtime as 'w)
      .groupBy('w,'a)
      .select('w.start() as 'w_start,'w.end() as 'w_end,'a,'b.sum() as 'b_sum)
      .execute()
      .print()

    /**
     * +----+-------------------------+-------------------------+-------------+-------------+
      | op |                 w_start |                   w_end |           a |       b_sum |
      +----+-------------------------+-------------------------+-------------+-------------+
      | +I |        2021-05-11T10:30 |        2021-05-11T11:30 |         100 |          33 |
      | +I |        2021-05-11T11:00 |        2021-05-11T12:00 |         100 |          42 |
      | +I |        2021-05-11T11:30 |        2021-05-11T12:30 |         100 |          38 |
      | +I |        2021-05-11T12:00 |        2021-05-11T13:00 |         100 |          29 |
      +----+-------------------------+-------------------------+-------------+-------------+
     */

  }

  @Test
  def mapFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sittings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tabEnv = StreamTableEnvironment.create(env,sittings)

    val tab1 = env.fromElements(
      "37c79b0653dab55657660253e73",
      "f4bf54d7b6bee928580e112e3cf",
      "a2e3c9e6554739f796587b7e3f1",
    ).toTable(tabEnv,'a)

    tabEnv.createTemporaryView("tab1",tab1)

//    tabEnv.executeSql(
//      """
//        |create table source_tab (
//        | name String
//        |) with (
//        | 'connector'='datagen'
//        |)
//        |""".stripMargin)

    // 一对一 标量函数
//    tabEnv.createFunction("one2one", classOf[One2OneMapFunc])
//    tabEnv.executeSql("select one2one(name) as new_name,name from tab1").print()

    val one2one = new One2OneMapFunc()
    tab1.map(one2one('a) ).as('new_name) // .map(call(classOf[One2OneMapFunc],'a)).as('new_name) 等效于
      .execute()
      .print()

    /**
     *  +----+--------------------------------+
        | op |                       new_name |
        +----+--------------------------------+
        | +I |                           37c7 |
        | +I |                           f4bf |
        | +I |                           a2e3 |
        +----+--------------------------------+
     */

    // 一对多 标量函数
    tabEnv.createFunction("one2multi",classOf[One2MultiMapFunc])
    tabEnv.executeSql(
      """select li.name1, li.name2, a from (
        | select one2multi(a) as li,a from tab1
        | ) temp
        |""".stripMargin
    ).print()
//    val one2multi = new One2MultiMapFunc()
//    tab1.map(one2multi('a)).as('name1,'name2)
//      .execute()
//      .print()

    /**
     * +----+--------------------------------+--------------------------------+
      | op |                          name1 |                          name2 |
      +----+--------------------------------+--------------------------------+
      | +I |                           37c7 |                           9b06 |
      | +I |                           f4bf |                           54d7 |
      | +I |                           a2e3 |                           c9e6 |
      +----+--------------------------------+--------------------------------+
     */
  }

  @Test
  def flatMapFunc(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tabEnv = BatchTableEnvironment.create(env)
    val tab1 = env.fromElements(
      ("a1",10,"java:98,python:20"),
      ("a2",20,"scala:99,python:50"),
      ("a3",20,null),
      ("a4",20,""),
    ).toTable(tabEnv,'id,'amount,'favors)

    tabEnv.createTemporaryView("tab1",tab1)

    val split = new MySplit()

    tabEnv.registerFunction("split",split)

    // 一行输入，多行输出
    tab1.flatMap(split('favors)).as('favor,'score) // as 需要写在 flatMap 外面
//      .select('id,'amount,'favor,'score) // 不能使用 select 了
      .execute()
      .print()

    /**
     * +--------------------------------+-------------+
      |                          favor |       score |
      +--------------------------------+-------------+
      |                           java |          98 |
      |                         python |          20 |
      |                          scala |          99 |
      |                         python |          50 |
      +--------------------------------+-------------+
     */

    // 表函数需要借助 lateral table 展开
    tabEnv.executeSql(
      """
        |select id,amount,favor,score from tab1,lateral table(split(favors)) as T(favor,score)
        |""".stripMargin
    ).print()

    /**
     * +--------------------------------+-------------+--------------------------------+-------------+
      |                             id |      amount |                          favor |       score |
      +--------------------------------+-------------+--------------------------------+-------------+
      |                             a1 |          10 |                           java |          98 |
      |                             a1 |          10 |                         python |          20 |
      |                             a2 |          20 |                          scala |          99 |
      |                             a2 |          20 |                         python |          50 |
      +--------------------------------+-------------+--------------------------------+-------------+
     */
  }

  @Test
  def aggFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val tab1 = env.fromElements(
      ("a1",10,"2021-05-11 12:00:01"),
      ("a1",9,"2021-05-11 12:20:01"),
      ("a1",21,"2021-05-11 12:40:01"),
      ("a1",12,"2021-05-11 13:10:01"),
      ("a1",13,"2021-05-11 13:40:01"),
    ).map{t=>
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      (t._1,t._2,sdf.parse(t._3).getTime)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String,Int,Long)](){
        override def extractTimestamp(t: (String, Int, Long), l: Long): Long = t._3
      })).toTable(tabEnv,'id,'amount,'rowtime.rowtime())

    tabEnv.createTemporaryView("tab1",tab1)

    val maxAndMin = new MyAgg()
    tab1.groupBy('id)
      .aggregate(call(maxAndMin,'amount) as ('max_amount,'min_amount)) // 与 map flatmap 不同，后面可以继续使用 select
//      .aggregate(maxAndMin('amount) as ('max_amount,'min_amount)) // 返回值为单值可以使用，多值 has 1 columns, whereas alias list has 2 columns
      .select('id,'max_amount,'min_amount)
      .execute()
      .print()

    tabEnv.registerFunction("max_min",maxAndMin)

    // TODO 使用 split(acc,',') 引出 acc 类型界定
    tabEnv.executeSql(
      """
        |select id,acc.f0 as max_amount,acc.f1 as min_amount from (
        | select id,max_min(amount) as acc from tab1 group by id
        |) temp
        |""".stripMargin
    ).print()

    /**
     *+----+--------------------------------+-------------+-------------+
      | op |                             id |  max_amount |  min_amount |
      +----+--------------------------------+-------------+-------------+
      | +I |                             a1 |          12 |          12 |
      | -U |                             a1 |          12 |          12 |
      | +U |                             a1 |          13 |          12 |
      | -U |                             a1 |          13 |          12 |
      | +U |                             a1 |          13 |          10 |
      | -U |                             a1 |          13 |          10 |
      | +U |                             a1 |          13 |           9 |
      | -U |                             a1 |          13 |           9 |
      | +U |                             a1 |          21 |           9 |
      +----+--------------------------------+-------------+-------------+
     */

    // 开窗 基于窗口聚合
    tab1.window(Tumble over 1.hours() on 'rowtime as 'w)
      .groupBy('w,'id)
      .aggregate(call(maxAndMin,'amount) as ('max_amount,'min_amount))
      .select('w.start() as 'w_start,'w.end() as 'w_end,'w.rowtime() as 'real_end,'id,'max_amount,'min_amount)
      .execute()
      .print()


    /**
     * +----+-------------------------+-------------------------+-------------------------+--------------------------------+-------------+-------------+
      | op |                 w_start |                   w_end |                real_end |                             id |  max_amount |  min_amount |
      +----+-------------------------+-------------------------+-------------------------+--------------------------------+-------------+-------------+
      | +I |        2021-05-11T04:00 |        2021-05-11T05:00 | 2021-05-11T04:59:59.999 |                             a1 |          21 |           9 |
      | +I |        2021-05-11T05:00 |        2021-05-11T06:00 | 2021-05-11T05:59:59.999 |                             a1 |          13 |          12 |
      +----+-------------------------+-------------------------+-------------------------+--------------------------------+-------------+-------------+
     */

    tabEnv.executeSql(
      """
        |select w_start,w_end,id,agg.f0 as max_amount,agg.f1 as min_amount from (
        |   select
        |     TUMBLE_START(rowtime, interval '1' hour) as w_start
        |     ,TUMBLE_END(rowtime, interval '1' hour) as w_end
        |     ,id
        |     ,max_min(amount) as agg
        |   from tab1
        |   group by TUMBLE(rowtime, interval '1' hour)
        |   ,id
        |) temp
        |""".stripMargin)

    /**
     * +----+-------------------------+-------------------------+-------------------------+--------------------------------+-------------+-------------+
      | op |                 w_start |                   w_end |                real_end |                             id |  max_amount |  min_amount |
      +----+-------------------------+-------------------------+-------------------------+--------------------------------+-------------+-------------+
      | +I |        2021-05-11T04:00 |        2021-05-11T05:00 | 2021-05-11T04:59:59.999 |                             a1 |          21 |           9 |
      | +I |        2021-05-11T05:00 |        2021-05-11T06:00 | 2021-05-11T05:59:59.999 |                             a1 |          13 |          12 |
      +----+-------------------------+-------------------------+-------------------------+--------------------------------+-------------+-------------+
     */

  }

  @Test
  def flatAggregate(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val top2 = new TopN

    tabEnv.registerFunction("top2",top2)

    val tab = env.fromElements( ("sensor_1", 1607527992000L, 1),
      ("sensor_1", 1607527992050L, 10),
      ("sensor_2", 1607527992000L, 2),
      ("sensor_2", 1607527994000L, 3),
      ("sensor_1", 1607527994050L, 9),
      ("sensor_2", 1607527995500L, 5),
      ("sensor_2", 1607527995550L, 24),
      ("sensor_2", 1607527996000L, 26),
      ("sensor_2", 1607527998000L, 11),
    ).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(3))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Long,Int)] {
        override def extractTimestamp(t: (String, Long, Int), l: Long): Long = t._2
      })).toTable(tabEnv,'id,'rowtime.rowtime(),'vc)

    tabEnv.createTemporaryView("tab",tab)

    // TableAggregateFunc 暂无 SQL 写法
    tab.groupBy("id")
            .flatAggregate(call("top2", $"vc").as("vcc", "rk"))
//      .flatAggregate("top2(vc) as (vcc,rk)")
      .select("id,vcc,rk") // group by 中和 flatAggregate 中没有出现过的字段不能出现
      .execute()
      .print()

    tab.window(Tumble over 5.seconds() on 'rowtime as 'w)
      .groupBy('w,'id)
      .flatAggregate("top2(vc) as (vcc,rk)")
      .select("w.start() as w_start,w.end() as w_end,id,vcc,rk")
      .execute()
      .print()


    /**
     * +----+-------------------------+-------------------------+--------------------------------+-------------+-------------+
      | op |                 w_start |                   w_end |                             id |         vcc |          rk |
      +----+-------------------------+-------------------------+--------------------------------+-------------+-------------+
      | +I |     2020-12-09T15:33:10 |     2020-12-09T15:33:15 |                       sensor_1 |          10 |           1 |
      | +I |     2020-12-09T15:33:10 |     2020-12-09T15:33:15 |                       sensor_1 |           9 |           2 |
      | +I |     2020-12-09T15:33:10 |     2020-12-09T15:33:15 |                       sensor_2 |           3 |           1 |
      | +I |     2020-12-09T15:33:10 |     2020-12-09T15:33:15 |                       sensor_2 |           2 |           2 |
      | +I |     2020-12-09T15:33:15 |     2020-12-09T15:33:20 |                       sensor_2 |          26 |           1 |
      | +I |     2020-12-09T15:33:15 |     2020-12-09T15:33:20 |                       sensor_2 |          24 |           2 |
      +----+-------------------------+-------------------------+--------------------------------+-------------+-------------+
     */

  }

}

class MySum extends AggregateFunction[Int,ListBuffer[Int]]{
  override def getValue(acc: ListBuffer[Int]): Int = acc.sum

  override def createAccumulator(): ListBuffer[Int] = ListBuffer[Int]()

  // 必须实现 accumulate
  def accumulate(acc: ListBuffer[Int], data:Int): Unit ={
    acc.append(data)
  }

}

// 一对一，标量函数 ，生成单列，map 中使用，一进必须有一处
class One2OneMapFunc extends ScalarFunction {

  def eval(input:String): String = {
    input.substring(0,4)
  }

  // 重载实现
  def eval(input:Object): String = {
    input.toString.substring(0,4)
  }

}

// 一对一，生成多列，通过 .xx 方式取出属性，map 中使用，一进必须有一处
@FunctionHint(output=new DataTypeHint("Row<name1 STRING,name2 STRING>"))
class One2MultiMapFunc extends ScalarFunction {

  def eval(input:String): Row = {
    Row.of(input.substring(0,4),input.substring(4,8))
  }
  // 有函数注解就可以省略 返回值类型
//  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
//    Types.ROW(Types.STRING(),Types.STRING())
//  }

}

// 一对多，表函数，一行输入，多行输出，有 collector 因此可以进行过滤
class MySplit(sep1:String=",",sep2:String=":") extends TableFunction[(String,Int)] {

  def eval(input:String): Unit = {
    if(input!=null){
      input.split(sep1).foreach {part =>
        if(part.size>0){
          val arr = part.split(sep2)
          collect((arr(0),arr(1).toInt))
        }
      }
    }
  }



}

case class MyMaxAndMin2(var max:JInt=Int.MinValue, var min:JInt=Int.MaxValue)

// 表函数只能使用  org.apache.flink.table.functions.AggregateFunction.AggregateFunction
// 聚合函数 一行输入
@FunctionHint(output=new DataTypeHint("Row<aa INT,bb INT>")) // AggregateFunction 中，属性命名不起作用
class MyAgg extends AggregateFunction[Row,MyMaxAndMin2]{

  // AggregateFunction 与 TableAggregateFunction本质区别在于，前者没有 collector，只有一次输出机会。后者有 collector 有多次输出机会
  // 因此 AggregateFunction 适合计算 极值、均值、求和，后者适合计算 TopN 开窗排序
  override def getValue(acc: MyMaxAndMin2): Row = Row.of(acc.max,acc.min)

  override def createAccumulator(): MyMaxAndMin2 = MyMaxAndMin2()

  def accumulate(acc:MyMaxAndMin2, data:JInt): Unit ={
    if(data<acc.min){
      acc.min = data
    }
    if(data>acc.max){
      acc.max = data
    }
  }

  def resetAccumulator(acc:MyMaxAndMin2): Unit ={
    acc.max = Int.MinValue
    acc.min = Int.MaxValue
  }

  override def getResultType(): TypeInformation[Row] = {
    Types.ROW(Types.INT(),Types.INT())
  }

}


// 表聚合函数，一行输入，多行输出，结合 group by 使用
class TopN(n:Int=2,desc:Boolean=true) extends TableAggregateFunction[(Int,Int), ListBuffer[Int]] {

  override def createAccumulator(): ListBuffer[Int] = {
    val list = ListBuffer[Int]()
    for(_ <- 0 until n){
      list.append(Int.MinValue)
    }
    list
  }

  def accumulate(acc: ListBuffer[Int], value: Int): Unit = {
    val factor = if(desc) 1 else -1
    acc.append(value)
    val temp = acc.sortWith(_ * factor > _ * factor).take(n)
    acc.clear()
    acc.appendAll(temp)
  }

  def merge(acc: ListBuffer[Int], it: java.lang.Iterable[ListBuffer[Int]]) {
    val iter = it.iterator()
    while (iter.hasNext) {
      iter.next().foreach(accumulate(acc, _))
    }
  }

  def emitValue(acc: ListBuffer[Int], out: Collector[(Int,Int)]): Unit = {
    for(i <- 0 until acc.size){
      if(acc(i)!= Int.MinValue){
        out.collect((acc(i), i+1))
      }
    }
  }
}

