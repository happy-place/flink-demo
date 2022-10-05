package com.bigdata.flink.proj.testing.taxi.scala

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.time.Time
import org.apache.flink.table.api.UnresolvedFieldExpression
import org.junit.Test
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, dataStreamConversions, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.DataTypes._
import org.apache.flink.table.types.DataType

import java.text.SimpleDateFormat
import java.time.Duration

class TableSQLFunc2 {

  // 匹配成功后跳转策略
  @Test
  def matchRecognize9(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val table = env.fromElements(
      ("XYZ", "2018-09-17 10:00:01", 7  , 1),
      ("XYZ", "2018-09-17 10:00:02", 9  , 2),
      ("XYZ", "2018-09-17 10:00:03", 10 , 1),
      ("XYZ", "2018-09-17 10:00:04", 5  , 2),
      ("XYZ", "2018-09-17 10:00:05", 10 , 2),
      ("XYZ", "2018-09-17 10:00:06", 7  , 2),
      ("XYZ", "2018-09-17 10:00:07", 14 , 2),
    ).map { t =>
      (t._1, sdf.parse(t._2).getTime, t._3, t._4)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String, Long, Int, Int)]() {
        override def extractTimestamp(t: (String, Long, Int, Int), l: Long): Long = t._2
      })).toTable(tabEnv, 'symbol, 'ctime.rowtime(), 'price, 'tax)

    tabEnv.createTemporaryView("Ticker", table)

    /**
     * AFTER MATCH SKIP PAST LAST ROW 跳转到最后一行之后，即B 之后
     *+----+--------------------------------+-------------+-------------------------+-------------------------+
      | op |                         symbol |   SUM_PRICE |              START_TIME |               LAST_TIME |
      +----+--------------------------------+-------------+-------------------------+-------------------------+
      | +I |                            XYZ |          26 |     2018-09-17T02:00:01 |     2018-09-17T02:00:04 |
      | +I |                            XYZ |          17 |     2018-09-17T02:00:05 |     2018-09-17T02:00:07 |
      +----+--------------------------------+-------------+-------------------------+-------------------------+
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   SUM(A.price) AS SUM_PRICE,
        |   FIRST(ctime) AS START_TIME,
        |   LAST(ctime) AS LAST_TIME
        |   ONE ROW PER MATCH
        |   AFTER MATCH SKIP PAST LAST ROW
        |   PATTERN(A+ B)
        |   DEFINE
        |     A AS SUM(A.price) < 30
        |) MR
        |""".stripMargin
    ).execute()
      .print()

    /**
     * AFTER MATCH SKIP TO NEXT ROW 跳转到下一行，即第二个A
     * +----+--------------------------------+-------------+-------------------------+-------------------------+
       | op |                         symbol |   SUM_PRICE |              START_TIME |               LAST_TIME |
       +----+--------------------------------+-------------+-------------------------+-------------------------+
       | +I |                            XYZ |          26 |     2018-09-17T02:00:01 |     2018-09-17T02:00:04 |
       | +I |                            XYZ |          24 |     2018-09-17T02:00:02 |     2018-09-17T02:00:05 |
       | +I |                            XYZ |          25 |     2018-09-17T02:00:03 |     2018-09-17T02:00:06 |
       | +I |                            XYZ |          22 |     2018-09-17T02:00:04 |     2018-09-17T02:00:07 |
       | +I |                            XYZ |          17 |     2018-09-17T02:00:05 |     2018-09-17T02:00:07 |
       +----+--------------------------------+-------------+-------------------------+-------------------------+
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   SUM(A.price) AS SUM_PRICE,
        |   FIRST(ctime) AS START_TIME,
        |   LAST(ctime) AS LAST_TIME
        |   ONE ROW PER MATCH
        |   AFTER MATCH SKIP TO NEXT ROW
        |   PATTERN(A+ B)
        |   DEFINE
        |     A AS SUM(A.price) < 30
        |) MR
        |""".stripMargin
    ).execute()
      .print()

    /**
     * AFTER MATCH SKIP TO LAST A 跳转到最后一行A
     * +----+--------------------------------+-------------+-------------------------+-------------------------+
       | op |                         symbol |   SUM_PRICE |              START_TIME |               LAST_TIME |
       +----+--------------------------------+-------------+-------------------------+-------------------------+
       | +I |                            XYZ |          26 |     2018-09-17T02:00:01 |     2018-09-17T02:00:04 |
       | +I |                            XYZ |          25 |     2018-09-17T02:00:03 |     2018-09-17T02:00:06 |
       | +I |                            XYZ |          17 |     2018-09-17T02:00:05 |     2018-09-17T02:00:07 |
       +----+--------------------------------+-------------+-------------------------+-------------------------+
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   SUM(A.price) AS SUM_PRICE,
        |   FIRST(ctime) AS START_TIME,
        |   LAST(ctime) AS LAST_TIME
        |   ONE ROW PER MATCH
        |   AFTER MATCH SKIP TO LAST A
        |   PATTERN(A+ B)
        |   DEFINE
        |     A AS SUM(A.price) < 30
        |) MR
        |""".stripMargin
    ).execute()
      .print()

    //   AFTER MATCH SKIP TO FIRST A 调回第一行，死循环，Could not skip to first element of a match.
//    tabEnv.sqlQuery(
//      """
//        |SELECT * FROM Ticker
//        |MATCH_RECOGNIZE (
//        | PARTITION BY symbol
//        | ORDER BY ctime
//        | MEASURES
//        |   SUM(A.price) AS SUM_PRICE,
//        |   FIRST(ctime) AS START_TIME,
//        |   LAST(ctime) AS LAST_TIME
//        |   ONE ROW PER MATCH
//        |   AFTER MATCH SKIP TO FIRST A
//        |   PATTERN(A+ B)
//        |   DEFINE
//        |     A AS SUM(A.price) < 30
//        |) MR
//        |""".stripMargin
//    ).execute()
//      .print()

  }

  // 限制内存
  @Test
  def matchRecognize10(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val table = env.fromElements(
      ("XYZ", "2018-09-17 10:00:01", 9  , 1),
      ("XYZ", "2018-09-17 10:00:02", 10  , 2),
      ("XYZ", "2018-09-17 10:00:03", 11 , 1),
      ("XYZ", "2018-09-17 10:00:04", 21 , 2),
      ("XYZ", "2018-09-17 10:00:05", 22 , 2),
      ("XYZ", "2018-09-17 10:00:06", 23 , 2),
      ("XYZ", "2018-09-17 10:00:07", 24 , 2),
    ).map { t =>
      (t._1, sdf.parse(t._2).getTime, t._3, t._4)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String, Long, Int, Int)]() {
        override def extractTimestamp(t: (String, Long, Int, Int), l: Long): Long = t._2
      })).toTable(tabEnv, 'symbol, 'ctime.rowtime(), 'price, 'tax)

    tabEnv.createTemporaryView("Ticker", table)

    /**
     * 开放条件很难收敛，应该尽可能添加约束条件，减少状态保存
     *   PATTERN(A B+ C)
         DEFINE
           A AS A.price > 10,
           C AS C.price > 20
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   SUM(A.price) AS SUM_PRICE,
        |   FIRST(ctime) AS START_TIME,
        |   LAST(ctime) AS LAST_TIME
        |   ONE ROW PER MATCH
        |   AFTER MATCH SKIP PAST LAST ROW
        |   PATTERN(A B+ C)
        |   DEFINE
        |     A AS A.price > 10,
        |     C AS C.price > 20
        |) MR
        |""".stripMargin
    ).execute()
      .print()

    /*
      (10,20] 形成封闭区间，且C只有一个，容易收敛
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   SUM(A.price) AS SUM_PRICE,
        |   FIRST(ctime) AS START_TIME,
        |   LAST(ctime) AS LAST_TIME
        |   ONE ROW PER MATCH
        |   AFTER MATCH SKIP PAST LAST ROW
        |   PATTERN(A B+ C)
        |   DEFINE
        |     A AS A.price > 10,
        |     B AS B.price <=20
        |     C AS C.price > 20
        |) MR
        |""".stripMargin
    ).execute()
      .print()

    /**
     * B 限制在出现0次或1次，也比较容易收敛
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   SUM(A.price) AS SUM_PRICE,
        |   FIRST(ctime) AS START_TIME,
        |   LAST(ctime) AS LAST_TIME
        |   ONE ROW PER MATCH
        |   AFTER MATCH SKIP PAST LAST ROW
        |   PATTERN(A B? C)
        |   DEFINE
        |     A AS A.price > 10,
        |     C AS C.price > 20
        |) MR
        |""".stripMargin
    ).execute()
      .print()

  }

  /**
   * 设置状态生命周期，及时删除用不到的状态，回收存储资源
   * 1.状态逐渐被写入checkpoint中，之后就不会被用到，因此要及时回收存储；
   * 2.状态被清空后，如果后面又来了属于这个状态的数据，状态按0处理；
   * 3.setIdleStateRetentionTime 最大，最小留存时间要＞5min
   */
  @Test
  def setIdleStateRetentionTime(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val tabConfig = tabEnv.getConfig
    // 状态在未更新状态下最少保存12h,最多保存24h
    tabConfig.setIdleStateRetentionTime(Time.hours(12 ),Time.hours(24))
    tabConfig.setIdleStateRetention(Duration.ofHours(24)) // 未更新时强制保存24h，然删除

  }

  def dataTypes(): Unit ={
    val t: DataType = INTERVAL(DAY(), SECOND(3));

  }

}
