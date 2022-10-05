package com.bigdata.flink.proj.testing.taxi.scala

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigdata.flink.proj.common.func.CommonUtil.{getClassPath, getResourcePath}
import com.bigdata.flink.proj.common.func.StreamSourceMock
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic, datastream, environment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, dataStreamConversions, _}
import org.apache.flink.table.api._
import org.apache.flink.table.catalog.GenericInMemoryCatalog
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.{Row, RowKind}
import org.junit.Test
import com.esotericsoftware.kryo.Serializer
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.table.sinks.CsvTableSink.CsvFormatter
import org.apache.flink.table.sources.{DefinedProctimeAttribute, StreamTableSource}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{Duration, Instant, ZoneId}
import java.util
import java.util.{Date, Map, Properties}
import scala.collection.mutable.ListBuffer
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters.seqAsJavaListConverter
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * @author: huhao18@meituan.com
 * @date: 2021/6/10 2:32 下午 
 * @desc:
 *
 */
class QueryTest {

  /**
   * ds 注册为表 或 转换为表
   * 自定义 csv 类型 sink
   */
  @Test
  def specifyQuery(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val ds = env.fromElements(
      (1,"a",10)
      ,(2,"b",20)
      ,(3,"c",30)
      ,(1,"a",11)
    )

    val Orders = ds.toTable(tabEnv, 'user, 'product, 'amount)
    val table = tabEnv.sqlQuery(s"select user,product,amount from $Orders where product like 'a%'") // 字符串变量引用

    tabEnv.createTemporaryView("Orders",ds,'user,'product,'amount)

    val table2 = tabEnv.sqlQuery(s"select user,product,amount from Orders where product like 'a%'") // 直接引用注册的表

    val schema = new Schema()
      .field("product",DataTypes.STRING())
      .field("amount",DataTypes.INT())

    tabEnv.connect(new FileSystem().path(getResourcePath("output/order.csv")))
      .withFormat(new Csv().fieldDelimiter(',').deriveSchema())
      .withSchema(schema)
      .createTemporaryTable("OrdersA")

    tabEnv.executeSql(s"insert into OrdersA select product,amount from $table2").await() // 必须有 await 才会执行
  }

  @Test
  def iter(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val ds = env.fromElements(
      (1,"a",10)
      ,(2,"b",20)
      ,(3,"c",30)
      ,(1,"a",11)
    )

    // sqlQuery 执行获取 table 对象，执行 execute 才能输出
    val Orders = ds.toTable(tabEnv, 'user, 'product, 'amount)
    tabEnv.sqlQuery(s"select user,product,amount from $Orders where product like 'a%'")
      .execute()
      .print()

    tabEnv.createTemporaryView("Orders",ds,'user,'product,'amount)

    // executeSql 执行生成 TableResult 对象，然后输出
    val tabResult = tabEnv.executeSql(s"select user,product,amount from Orders where product like 'a%'")
    val iterator = tabResult.collect()
    try while(iterator.hasNext){
      val row = iterator.next()
      println(row)
    }finally{
      iterator.close()
    }
  }

  @Test
  def selectWithoutFrom(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.sqlQuery("select 1 as a,'jack' as b").execute().print()
  }


  /**
   * union 去重
   */
  @Test
  def union(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.sqlQuery(
      """
        |select 1 as a
        |union
        |select 1 as a
        |""".stripMargin
    ).execute().print()
  }

  /**
   * union all 不去重
   */
  @Test
  def unionAll(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.sqlQuery(
      """
        |select 1 as a
        |union all
        |select 1 as a
        |""".stripMargin
    ).execute().print()
  }

  /**
   * 差集
   * except 去除 select 1 as a
   * +----+-------------+
   * | op |           a |
   * +----+-------------+
   * | +I |           2 |
   * +----+-------------+
   */
  @Test
  def except(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.sqlQuery(
      """
        |select 1 as a
        |union
        |select 2 as a
        |except
        |select 1 as a
        |""".stripMargin
    ).execute().print()
  }

  /**
   * intersect 交集
   * +----+-------------+
   * | op |           a |
   * +----+-------------+
   * | +I |           1 |
   * | +I |           2 |
   * +----+-------------+
   */
  @Test
  def intersect(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.sqlQuery(
      """
        |select a from (
        | select 1 as a
        | union
        | select 2 as a
        | union
        | select 4 as a
        |) temp
        |intersect
        |select a from (
        | select 1 as a
        | union all
        | select 2 as a
        | union all
        | select 3 as a
        |) temp
        |""".stripMargin
    ).execute().print()
  }

  // 包含
  @Test
  def in(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val tab1 = tabEnv.sqlQuery(
      """
        | select 1 as a
        | union
        | select 2 as a
        | union
        | select 4 as a
        |""".stripMargin)

    val tab2 = tabEnv.sqlQuery(
      """
        | select 1 as a
        | union all
        | select 2 as a
        | union all
        | select 3 as a
        |""".stripMargin)

    tabEnv.sqlQuery(
      s"""
        |select a from
        |$tab1
        |where a in (
        | select a from $tab2
        |)
        |""".stripMargin
    ).execute()
      .print()
  }


  /**
   * order by 排序
   * 排序字段必须包含时间字段，必须是 Datatime 类型或 long 类型，且时间字段必须是 order by 第一个字段，对于时间不能进行逆序排序，然后才可以接其他字段
   * +----+-------------------------+-------------+
   * | op |                 rowtime |      amount |
   * +----+-------------------------+-------------+
   * | +I | 1970-01-01T00:00:00.001 |         101 |
   * | +I | 1970-01-01T00:00:00.001 |         100 |
   * | +I | 1970-01-01T00:00:00.002 |         104 |
   * | +I | 1970-01-01T00:00:00.002 |         102 |
   * | +I | 1970-01-01T00:00:00.003 |         103 |
   * +----+-------------------------+-------------+
   * 5 rows in set
   *
   */
  @Test
  def orderBy(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val ds = env.fromElements(
      (1L,100)
      ,(2L,102)
      ,(1L,101)
      ,(3L,103)
      ,(2L,104)
    ).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(200))
      .withTimestampAssigner(new SerializableTimestampAssigner[(Long,Int)]{
        override def extractTimestamp(t: (Long,Int), l: Long): Long = t._1
      }))

//      .toTable(tabEnv,'rowtime.rowtime(),'amount)

    tabEnv.createTemporaryView("tab",ds,'rowtime.rowtime(),'amount)

    tabEnv.sqlQuery(
      """select * from tab
        | order by rowtime,amount desc
        |""".stripMargin
    ).execute().print()
  }

  /**
   * limit 3 显示 3 条，limit all 显示全部
   */
  @Test
  def limit(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.sqlQuery(
      """
        |select a from (
        | select 1 as a
        |   union
        | select 2 as a
        |   union
        | select 4 as a
        |   union
        | select 5 as a
        |   union
        | select 6 as a
        |) temp
        |limit all
        |""".stripMargin
    ).execute().print()
  }

  @Test
  def selectAll(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val tab1 = tabEnv.sqlQuery(
      """
        | select 1 as a,10 as b
        |   union all
        | select 1 as a,10 as b
        |   union all
        | select 4 as a,10 as b
        |   union all
        | select 5 as a,10 as b
        |   union all
        | select 6 as a,10 as b
        |""".stripMargin
    )

    // all * 等效于 *
    // distinct * 去重
    tabEnv.sqlQuery(s"select all * from $tab1")
      .execute()
      .print()

    tabEnv.sqlQuery(s"select distinct * from $tab1")
      .execute()
      .print()
  }

  /**
   * group by 分组，涉及窗口的，通常要带上窗口
   * having 对聚合结果进行过滤，不能直接引用聚合结果的别名 sum_score，而要使用聚合表达式 sum(score) > 10
   * +----+-------------------------+--------------------------------+--------------------------------+-------------+
   * | op |                   start |                          class |                            stu |   sum_score |
   * +----+-------------------------+--------------------------------+--------------------------------+-------------+
   * | +I |        1970-01-01T00:00 |                             c2 |                             s2 |          20 |
   * | +I |        1970-01-01T00:00 |                             c1 |                             s3 |          20 |
   * +----+-------------------------+--------------------------------+--------------------------------+-------------+
   */
  @Test
  def groupby(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val ds = env.fromElements(
       (1000L,"c1","s1",10)
      ,(2000L,"c1","s2",10)
      ,(3000L,"c1","s3",10)
      ,(4000L,"c1","s3",10)
      ,(1000L,"c2","s1",10)
      ,(1000L,"c2","s2",10)
      ,(2000L,"c2","s2",10)
    ).assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[(Long,String,String,Int)] {
        override def extractTimestamp(t: (Long, String, String, Int), l: Long): Long = t._1
      }))

    tabEnv.createTemporaryView("records",ds,'rowtime.rowtime(),'class,'stu,'score)

    tabEnv.sqlQuery(
      """
        |select
        | tumble_start(rowtime, interval '5' seconds) as `start`
        | ,class
        | ,stu
        | ,sum(score) as sum_score
        |from records
        |group by tumble(rowtime, interval '5' seconds),class,stu
        |having sum(score) > 10
        |""".stripMargin
    ).execute()
      .print()
  }

  /**
   * group by rollup(col1,col2,...)  从右往左聚合，知道 group by 维度消失
   * 注：不能使用 窗口不能参与rollup
   +----+--------------------------------+--------------------------------+-------------+
   | op |                          class |                            stu |   sum_score |
   +----+--------------------------------+--------------------------------+-------------+
   | +I |                             c1 |                             s2 |          10 |
   | +I |                             c1 |                             s1 |          10 |
   | -U |                             c1 |                             s1 |          10 |
   | +U |                             c1 |                             s1 |          20 |
   | +I |                             c2 |                             s2 |          10 |
   | -U |                             c2 |                             s2 |          10 |
   | +U |                             c2 |                             s2 |          20 |
   | +I |                             c1 |                         (NULL) |          10 |
   | -U |                             c1 |                         (NULL) |          10 |
   | +U |                             c1 |                         (NULL) |          20 |
   | -U |                             c1 |                         (NULL) |          20 |
   | +U |                             c1 |                         (NULL) |          30 |
   | +I |                             c2 |                             s1 |          10 |
   | +I |                             c2 |                         (NULL) |          10 |
   | -U |                             c2 |                         (NULL) |          10 |
   | +U |                             c2 |                         (NULL) |          20 |
   | -U |                             c2 |                         (NULL) |          20 |
   | +U |                             c2 |                         (NULL) |          30 |
   | +I |                         (NULL) |                         (NULL) |          10 |
   | -U |                         (NULL) |                         (NULL) |          10 |
   | +U |                         (NULL) |                         (NULL) |          20 |
   | -U |                         (NULL) |                         (NULL) |          20 |
   | +U |                         (NULL) |                         (NULL) |          30 |
   | -U |                         (NULL) |                         (NULL) |          30 |
   | +U |                         (NULL) |                         (NULL) |          40 |
   | -U |                         (NULL) |                         (NULL) |          40 |
   | +U |                         (NULL) |                         (NULL) |          50 |
   | -U |                         (NULL) |                         (NULL) |          50 |
   | +U |                         (NULL) |                         (NULL) |          60 |
   +----+--------------------------------+--------------------------------+-------------+
   */
  @Test
  def rullup(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val ds = env.fromElements(
       ("c1","s1",10)
      ,("c1","s1",10)
      ,("c1","s2",10)
      ,("c2","s1",10)
      ,("c2","s2",10)
      ,("c2","s2",10)
    )

    tabEnv.createTemporaryView("records",ds,'class,'stu,'score)

    tabEnv.sqlQuery(
      """
        |select
        | class
        | ,stu
        | ,sum(score) as sum_score
        |from records
        |group by
        |rollup (class,stu)
        |""".stripMargin
    ).execute()
      .print()
  }

  /**
   * group by cube(col1,col2,...) 先从右往左聚合，后从左往右聚合，直至最后全部维度消失
   * +----+--------------------------------+--------------------------------+-------------+
  | op |                          class |                            stu |   sum_score |
  +----+--------------------------------+--------------------------------+-------------+
  | +I |                             c1 |                             s2 |          10 |
  | +I |                             c1 |                             s1 |          10 |
  | -U |                             c1 |                             s1 |          10 |
  | +U |                             c1 |                             s1 |          20 |
  | +I |                             c1 |                         (NULL) |          10 |
  | -U |                             c1 |                         (NULL) |          10 |
  | +U |                             c1 |                         (NULL) |          20 |
  | -U |                             c1 |                         (NULL) |          20 |
  | +U |                             c1 |                         (NULL) |          30 |
  | -U |                         (NULL) |                             s1 |          10 |
  | +U |                         (NULL) |                             s1 |          20 |
  | -U |                         (NULL) |                             s1 |          20 |
  | +U |                         (NULL) |                             s1 |          30 |
  | +I |                         (NULL) |                             s1 |          10 |
  | +I |                             c2 |                             s1 |          10 |
  | +I |                             c2 |                             s2 |          10 |
  | -U |                             c2 |                             s2 |          10 |
  | +U |                             c2 |                             s2 |          20 |
  | +I |                             c2 |                         (NULL) |          10 |
  | -U |                             c2 |                         (NULL) |          10 |
  | +U |                             c2 |                         (NULL) |          20 |
  | -U |                             c2 |                         (NULL) |          20 |
  | +U |                             c2 |                         (NULL) |          30 |
  | +I |                         (NULL) |                             s2 |          10 |
  | -U |                         (NULL) |                             s2 |          10 |
  | +U |                         (NULL) |                             s2 |          20 |
  | -U |                         (NULL) |                             s2 |          20 |
  | +U |                         (NULL) |                             s2 |          30 |
  | +I |                         (NULL) |                         (NULL) |          10 |
  | -U |                         (NULL) |                         (NULL) |          10 |
  | +U |                         (NULL) |                         (NULL) |          20 |
  | -U |                         (NULL) |                         (NULL) |          20 |
  | +U |                         (NULL) |                         (NULL) |          30 |
  | -U |                         (NULL) |                         (NULL) |          30 |
  | +U |                         (NULL) |                         (NULL) |          40 |
  | -U |                         (NULL) |                         (NULL) |          40 |
  | +U |                         (NULL) |                         (NULL) |          50 |
  | -U |                         (NULL) |                         (NULL) |          50 |
  | +U |                         (NULL) |                         (NULL) |          60 |
  +----+--------------------------------+--------------------------------+-------------+
   */
  @Test
  def cube(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val ds = env.fromElements(
      ("c1","s1",10)
      ,("c1","s1",10)
      ,("c1","s2",10)
      ,("c2","s1",10)
      ,("c2","s2",10)
      ,("c2","s2",10)
    )
    tabEnv.createTemporaryView("records",ds,'class,'stu,'score)

    tabEnv.sqlQuery(
      """
        |select
        | class
        | ,stu
        | ,sum(score) as sum_score
        |from records
        |group by
        |cube (class,stu)
        |""".stripMargin
    ).execute()
      .print()
  }

  /**
   * group by grouping sets((col1),(col1,col2))
   * 按给定维度组合进行 分组聚合
   * +----+--------------------------------+--------------------------------+-------------+
     | op |                          class |                            stu |   sum_score |
     +----+--------------------------------+--------------------------------+-------------+
     | +I |                             c1 |                             s2 |          10 |
     | +I |                             c2 |                             s2 |          10 |
     | -U |                             c2 |                             s2 |          10 |
     | +U |                             c2 |                             s2 |          20 |
     | +I |                             c1 |                             s1 |          10 |
     | -U |                             c1 |                             s1 |          10 |
     | +U |                             c1 |                             s1 |          20 |
     | +I |                             c2 |                             s1 |          10 |
     | +I |                             c1 |                         (NULL) |          10 |
     | -U |                             c1 |                         (NULL) |          10 |
     | +U |                             c1 |                         (NULL) |          20 |
     | -U |                             c1 |                         (NULL) |          20 |
     | +U |                             c1 |                         (NULL) |          30 |
     | +I |                             c2 |                         (NULL) |          10 |
     | -U |                             c2 |                         (NULL) |          10 |
     | +U |                             c2 |                         (NULL) |          20 |
     | -U |                             c2 |                         (NULL) |          20 |
     | +U |                             c2 |                         (NULL) |          30 |
     +----+--------------------------------+--------------------------------+-------------+
   */
  @Test
  def groupingSets(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val ds = env.fromElements(
      ("c1","s1",10)
      ,("c1","s1",10)
      ,("c1","s2",10)
      ,("c2","s1",10)
      ,("c2","s2",10)
      ,("c2","s2",10)
    )
    tabEnv.createTemporaryView("records",ds,'class,'stu,'score)

    tabEnv.sqlQuery(
      """
        |select
        | class
        | ,stu
        | ,sum(score) as sum_score
        |from records
        |group by
        |grouping sets((class),(class,stu))
        |""".stripMargin
    ).execute()
      .print()
  }



  @Test
  def join(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val tab1 = tabEnv.sqlQuery(
      """
        |select 1 as a,10 as b
        | union all
        |select 2 as a,20 as b
        | union all
        |select 3 as a,30 as b
        |""".stripMargin)

    val tab2 = tabEnv.sqlQuery(
      """
        |select 1 as a,100 as c
        | union all
        |select 2 as a,200 as c
        | union all
        |select 4 as a,400 as c
        |""".stripMargin)

    // inner join
    tabEnv.sqlQuery(
      s"""
        |select
        | t1.a,b,c
        |from
        |$tab1 as t1
        |join
        |$tab2 as t2
        |on t1.a = t2.a
        |""".stripMargin
    ).execute()
      .print()

    // oracle 风格 join
    tabEnv.sqlQuery(
      s"""
         |select
         | t1.a,b,c
         |from
         |$tab1 as t1
         |join
         |$tab2 as t2
         |using (a)
         |""".stripMargin
    ).execute()
      .print()

    // left join
    tabEnv.sqlQuery(
      s"""
         |select
         | t1.a,b,c
         |from
         |$tab1 as t1
         |left join
         |$tab2 as t2
         |on t1.a = t2.a
         |""".stripMargin
    ).execute()
      .print()

    // right join
    tabEnv.sqlQuery(
      s"""
         |select
         | t2.a,b,c
         |from
         |$tab1 as t1
         |right join
         |$tab2 as t2
         |on t1.a = t2.a
         |""".stripMargin
    ).execute()
      .print()

    // full join
    tabEnv.sqlQuery(
      s"""
         |select
         | coalesce(t1.a,t2.a) as a,b,c
         |from
         |$tab1 as t1
         |full join
         |$tab2 as t2
         |on t1.a = t2.a
         |""".stripMargin
    ).execute()
      .print()

    // corss join 又称 自然连接，等效于 ',' 逗号连接
    tabEnv.sqlQuery(
      s"""
         |select
         |a,b
         |from
         |(
         |select 1 as a
         |union all
         |select 2 as a
         |) t1
         |natural join
         |(
         |select 'A1' as b
         |union all
         |select 'A2' as b
         |) t2
         |
         |""".stripMargin
    ).execute()
      .print()
  }

  /**
   * lateral table
   * 表函数，一行输入，多行输出
   * +----+-------------+--------------------------------+--------------------------------+
   * | op |           a |                              b |                          label |
   * +----+-------------+--------------------------------+--------------------------------+
   * | +I |           1 |                     hello,java |                          hello |
   * | +I |           1 |                     hello,java |                           java |
   * | +I |           2 |                    python,java |                         python |
   * | +I |           2 |                    python,java |                           java |
   * +----+-------------+--------------------------------+--------------------------------+
   */
  @Test
  def lateralTable(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.createFunction("split",classOf[Split])

    val tab1 = tabEnv.sqlQuery(
      """
        |select 1 as a,'hello,java' as b
        |union all
        |select 2 as a,'python,java' as b
        |""".stripMargin
    )

    // t 为表名，label 为字段名
    tabEnv.sqlQuery(
      s"""
        |select a,b,label
        |from
        |$tab1,
        |lateral table(split(b)) as t(label)
        |""".stripMargin
    ).execute()
      .print()
  }

  /**
   * unnest(b) as t(bb) 将数组 b 每个元素拆分，一行输入，多行 输出
   * +----+--------------------------------+--------------------------------+-------------+
    | op |                              a |                              b |          bb |
    +----+--------------------------------+--------------------------------+-------------+
    | +I |                              a |                         [1, 2] |           1 |
    | +I |                              a |                         [1, 2] |           2 |
    | +I |                              b |                         [2, 3] |           2 |
    | +I |                              b |                         [2, 3] |           3 |
    +----+--------------------------------+--------------------------------+-------------+
   */
  @Test
  def unnest(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val ds = env.fromElements(
      ("a",Array(1,2))
      ,("b",Array(2,3))
    )

    tabEnv.createTemporaryView("tab",ds,'a,'b)

    tabEnv.sqlQuery(
      """
        |select a,b,bb
        |from tab,
        |unnest(b) as t(bb)
        |""".stripMargin
    ).execute()
      .print()
  }

  /**
   * 实时表与时态表 join
   * FOR SYSTEM_TIME AS OF 表明查时态表的入参时间
   */
  @Test
  def temporalJoin(): Unit ={
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env, settings)

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
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        override def extractTimestamp(t: (Long, String, Long), l: Long): Long = t._3
      })
    ).toTable(tabEnv,'amount,'currency,'rowtime.rowtime())

    tabEnv.createTemporaryView("Orders",orders)

    /**
     * with 'json' format doesn't support defining PRIMARY KEY constraint on the table
     */
    tabEnv.executeSql(
      """
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

    //    tabEnv.sqlQuery("select * from Orders").executeAndCollect().foreach(println(_))
    //    tabEnv.sqlQuery("select * from RatesHistory").executeAndCollect().foreach(println(_))

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
        |LEFT JOIN RatesHistory FOR SYSTEM_TIME AS OF o.rowtime as r
        |ON o.currency = r.currency
        |""".stripMargin
    ).execute()
      .print()
  }

  /**
   * 定义窗口，然后基于创建进行 sum 累计
   * 每新增一个元素，就基于扩充一次窗口
   *
   */
  @Test
  def window(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val ds = env.fromElements(
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
    )

    tabEnv.createTemporaryView("orders",ds,'a,'b,'c,'rowtime.rowtime())

    tabEnv.sqlQuery(
      """
        |select
        | a
        | ,sum(distinct b) over w -- 重复的不参与计算，基于后面的窗口进行计算
        |from orders
        |window w as (
        | partition by a -- 分区
        | order by rowtime -- 时间字段排序
        | ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW -- 从头累加到当前行
        |)
        |""".stripMargin
    ).execute()
      .print()

    /**
     * +----+--------------------------------+-------------+
      | op |                              a |      EXPR$1 |
      +----+--------------------------------+-------------+
      | +I |                             a1 |          11 |
      | +I |                             a1 |          11 |  <<< 重复
      | +I |                             a1 |          41 |
      | +I |                             a1 |          51 |
      +----+--------------------------------+-------------+
     */

    tabEnv.sqlQuery(
      """
        |select
        | a
        | ,sum(b) over w -- 重复的不参与计算，基于后面的窗口进行计算
        |from orders
        |window w as (
        | partition by a -- 分区
        | order by rowtime -- 时间字段排序
        | ROWS BETWEEN 2 PRECEDING AND CURRENT ROW -- 当前行往前追 2 行(总共 3 行)累计求和
        |)
        |""".stripMargin
    ).execute()
      .print()

    // 等价于下面的
    tabEnv.sqlQuery(
      """
        |select
        | a
        | ,sum(b) over(
        |   partition by a -- 分区
        |   order by rowtime -- 时间字段排序
        |   ROWS BETWEEN 2 PRECEDING AND CURRENT ROW -- 当前行往前追 2 行(总共 3 行)累计求和
        | )
        |from orders
        |""".stripMargin
    ).execute()
      .print()

    /**
     * +----+--------------------------------+-------------+
      | op |                              a |      EXPR$1 |
      +----+--------------------------------+-------------+
      | +I |                             a1 |          11 |
      | +I |                             a1 |          22 |
      | +I |                             a1 |          52 |
      | +I |                             a1 |          51 |
      +----+--------------------------------+-------------+
     */
  }

  @Test
  def matchRecognize(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val table = env.fromElements(
      ("ACME", "2021-05-11 10:00:00", 12, 1),
      ("ACME", "2021-05-11 10:00:01", 17, 2),
      ("ACME", "2021-05-11 10:00:02", 19, 1),
      ("ACME", "2021-05-11 10:00:03", 21, 3),
      ("ACME", "2021-05-11 10:00:04", 25, 2),
      ("ACME", "2021-05-11 10:00:05", 18, 1),
      ("ACME", "2021-05-11 10:00:06", 15, 1),
      ("ACME", "2021-05-11 10:00:07", 14, 2),
      ("ACME", "2021-05-11 10:00:08", 24, 2),
      ("ACME", "2021-05-11 10:00:09", 25, 2),
      ("ACME", "2021-05-11 10:00:10", 19, 1)
    ).map{t =>
      (t._1,sdf.parse(t._2).getTime,t._3,t._4)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String,Long,Int,Int)](){
        override def extractTimestamp(t: (String, Long, Int, Int), l: Long): Long = t._2
      })).toTable(tabEnv, 'symbol, 'rowtime.rowtime(), 'price, 'tax)
    // 事件时间 必须声明时间字段和水印

    tabEnv.createTemporaryView("Ticker",table)

    /**
     * MATCH_RECOGNIZE() 开启模式匹配
     * PARTITION BY 分区
     * ORDER BY 分区内排序，只能时间字段在前，且基于时间只能升序排序
     * MEASURES 识别成功后提取的信息，类似于 select 后面的字段，以逗号分割多列
     * ONE ROW PER MATCH 一次成功匹配输出一行记录
     * AFTER MATCH SKIP TO 一次成功匹配后跳转到哪里，开启下次匹配，决定已经匹配的记录能否被复用
     * PATTERN 声明匹配模式
     * DEFINE 定义前面声明的匹配模式，以逗号分割
     *
     *  输出：Partition Columns + Measures Columns
     * SELECT *
     *   FROM Ticker  -- 从 Tricker 表取数据灌入下面的匹配逻辑
     *     MATCH_RECOGNIZE (  -- 开启匹配模式
     *      PARTITION BY symbol -- 基于 symbol 分区，可以保证并行计算，否则就转成全局排序
     *      ORDER BY rowtime -- 基于事件时间排序(升序)
     *      MEASURES -- 定义匹配成功后输出内容
     *       START_ROW.rowtime as start_tmstp, -- 起始行的时间
     *       LAST(PRICE_DOWN.rowtime) as bottom_tmstp, -- 价格下降到最低点的时间，由于下面定义了连续价格下跌，因此这里去 Last 最后一个
     *       PRICE_UP.rowtime as end_tmstp -- 价格上涨时间（即结束 匹配时间)
     *      ONE ROW PER MATCH  -- 每次成功匹配，按上面组织的字段输出一行记录
     *      AFTER MATCH SKIP TO LAST PRICE_UP -- 成功匹配后跳转到 最后一个价格上升点 开始下次匹配
     *      PATTERN (START_ROW PRICE_DOWN+ PRICE_UP) -- 声明匹配项 （类似于定义一个变量，每个对应Ticker表一条记录，PRICE_DOWN+ 表明连续多个价格下跌时间)
     *      DEFINE -- 对上面声明的匹配项，填充匹配规则 （给变量赋值)
     *        PRICE_DOWN AS
     *          (LAST(PRICE_DOWN.price,1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR  -- LAST(PRICE_DOWN.price,1) IS NULL 之前不存在，首次下跌为 之前不存在下跌，当前价格低于起始价格
     *          PRICE_DOWN.price < LAST(PRICE_DOWN.price,1), -- 或 之前存在下跌(略)，且当前价格比上次下跌价格还低
     *        PRICE_UP AS
     *          PRICE_UP.price > LAST(PRICE_DOWN.price,1)  -- 当前价格比上次价格下跌高
     *      ) MR -- MATCH_RECOGNIZE 结束标签
     */

    tabEnv.sqlQuery(
      """
        |SELECT *
        |FROM Ticker
        |  MATCH_RECOGNIZE (
        |   PARTITION BY symbol
        |   ORDER BY rowtime
        |   MEASURES
        |    START_ROW.rowtime as start_tmstp,
        |    LAST(PRICE_DOWN.rowtime) as bottom_tmstp,
        |    PRICE_UP.rowtime as end_tmstp
        |   ONE ROW PER MATCH
        |   AFTER MATCH SKIP TO LAST PRICE_UP
        |   PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)
        |   DEFINE
        |     PRICE_DOWN AS
        |       (LAST(PRICE_DOWN.price,1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR
        |       PRICE_DOWN.price < LAST(PRICE_DOWN.price,1),
        |     PRICE_UP AS
        |       PRICE_UP.price > LAST(PRICE_DOWN.price,1)
        |   ) MR
        |""".stripMargin
    ).execute()
      .print()
  }


  /**
   * interval join 三角 join
   * +----+--------------------------------+-------------+-------------+-------------------------+
    | op |                           name |          r1 |          m2 |                     rt1 |
    +----+--------------------------------+-------------+-------------+-------------------------+
    | +I |                            Bob |          10 |         100 |     2021-05-11T12:00:01 |
    | +I |                          Alice |          12 |         130 |     2021-05-11T12:30:01 |
    | +I |                           Lily |          14 |         140 |     2021-05-11T13:20:01 |
    +----+--------------------------------+-------------+-------------+-------------------------+
   */
  @Test
  def intervalJoin(): Unit ={
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
    ).toTable(tabEnv,'name,'rate,'rowtime.rowtime())

    val tab2 = env.fromElements(
      ("Bob",100,"2021-05-11 12:00:01"),
      ("Lily",120,"2021-05-11 12:30:01"),
      ("Alice",130,"2021-05-11 13:00:01"),
      ("Lily",140,"2021-05-11 13:20:01"),
      ("Tom",140,"2021-05-11 13:30:01"),
    ).map{t =>
      (t._1,t._2,sdf.parse(t._3).getTime + 8*3600*1000)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String,Int,Long)] {
        override def extractTimestamp(t: (String, Int, Long), l: Long): Long = t._3
      })
    ).toTable(tabEnv,'name,'amount,'rowtime.rowtime())

    tabEnv.sqlQuery(
      s"""
        |select
        | t1.name
        | ,t1.rate as r1
        | ,t2.amount as m2
        | ,t1.rowtime as rt1
        |from
        | $tab1 as t1
        |,$tab2 as t2
        |where t1.name = t2.name
        |and t1.rowtime >= t2.rowtime - interval '30' minute
        |and t1.rowtime < t2.rowtime + interval '30' minute
        |""".stripMargin
    ).execute()
      .print()

  }

  @Test
  def buildIn(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    // 不同
    tabEnv.sqlQuery(
      """
        |select 1 is distinct from 1
        |union all
        |select 1 is not distinct from 1
        |""".stripMargin
    ).execute()
      .print()

    /**
     *   +----+--------+
         | op | EXPR$0 |
         +----+--------+
         | +I |  false |
         | +I |   true |
         +----+--------+
     */
  }

  /**
   * row_number() over()
   * 开窗排序 取 最大 或 最小 topN, 目前只实现了 row_numver，暂不支持 dense_rank、rank
   * 区别：row_number 排名连续，相同分数不同排名
   * rank 排名不连续，相同分数，相同排名，之后名称会跳跃
   * dense_rank 排名连续，相同分数相同排名，之后名称是连续的
   *
   * 注：最外层输出时，建议尽量不输出 rk 排名，避免操作记录过多
   *
   * 最外层 select b,rk 输出了 rk 排名导致修改排名，操作次数增加
   *+----+-------------+----------------------+
    | op |           b |                   rk |
    +----+-------------+----------------------+
    | +I |           1 |                    1 |
    | -U |           1 |                    1 |
    | +U |           3 |                    1 |
    | +I |           1 |                    2 |
    | -U |           1 |                    2 |
    | +U |           2 |                    2 |
    | +I |           1 |                    3 |
    | -U |           3 |                    1 |
    | +U |           5 |                    1 |
    | -U |           2 |                    2 |
    | +U |           3 |                    2 |
    | -U |           1 |                    3 |
    | +U |           2 |                    3 |
    | -U |           3 |                    2 |
    | +U |           4 |                    2 |
    | -U |           2 |                    3 |
    | +U |           3 |                    3 |
    | -U |           5 |                    1 |
    | +U |           7 |                    1 |
    | -U |           4 |                    2 |
    | +U |           5 |                    2 |
    | -U |           3 |                    3 |
    | +U |           4 |                    3 |
    | -U |           5 |                    2 |
    | +U |           6 |                    2 |
    | -U |           4 |                    3 |
    | +U |           5 |                    3 |
    +----+-------------+----------------------+
    27 rows in set

    最外层 select b 不输出排名，只对收尾进行操作，操作次数明显减少，N 越大，效果月明细
    +----+-------------+
    | op |           b |
    +----+-------------+
    | +I |           3 |
    | +I |           4 |
    | +I |           1 |
    | -D |           1 |
    | +I |           2 |
    | -D |           2 |
    | +I |           7 |
    | -D |           3 |
    | +I |           5 |
    | -D |           4 |
    | +I |           6 |
    +----+-------------+
    11 rows in set

   */
  @Test
  def topN(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val ds = env.fromElements(
      1,2,3,4,5,6,7
    ).map((1,_))
    env.setParallelism(1)
    tabEnv.createTemporaryView("tab",ds,'a,'b)

    tabEnv.sqlQuery(
      """
        |select
        |b,rk
        |from (
        | select
        |   a
        |   ,b
        |   ,row_number() over(
        |     partition by a
        |     order by b desc
        |   ) as rk
        | from tab
        |) temp
        |where rk <=3
        |""".stripMargin
    ).execute()
      .print()
  }

  /**
   * 基于开窗 rk = 1 解决重复问题，即使记录完全重复，也会按不同名称排名，记录如果咋时间上存在先后顺序，就取最早或最迟一条
   * +----+-------------+-------------+-------------+
    | op |           a |           b |           c |
    +----+-------------+-------------+-------------+
    | +I |           1 |           2 |          10 |
    +----+-------------+-------------+-------------+
   */
  @Test
  def duplicate(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val ds = env.fromElements(
      (1,2,10)
      ,(1,2,10)
      ,(1,3,10)
      ,(1,4,10)
    )
    tabEnv.createTemporaryView("tab",ds,'a,'b,'c)

    tabEnv.sqlQuery(
      """
        |select
        |a,b,c
        |from (
        | select a,b,c
        | ,row_number() over(
        |   partition by a
        |   order by b
        | ) as rk
        | from tab
        |) temp
        |where rk = 1
        |""".stripMargin
    ).execute()
      .print()


  }




}

// TableFunction 中定义的是输出类型
class Split extends TableFunction[(String)]{

  def eval(input:String):Unit = {
    input.split(",").foreach{
      collect(_)
    }
  }
}
