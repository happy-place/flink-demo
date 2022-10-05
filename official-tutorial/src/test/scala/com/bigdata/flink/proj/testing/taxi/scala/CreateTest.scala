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
import org.apache.flink.table.catalog.{Catalog, CatalogDatabase, CatalogDatabaseImpl, GenericInMemoryCatalog}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.{Row, RowKind}
import org.junit.Test
import com.esotericsoftware.kryo.Serializer
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment
import org.apache.flink.table.annotation.{DataTypeHint, FunctionHint}
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.functions.{ScalarFunction, TableFunction}
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
import scala.collection.JavaConverters._
import scala.util.Random
/**
 * @author: huhao18@meituan.com
 * @date: 2021/6/11 9:56 上午 
 * @desc:
 *
 */
class CreateTest {

  /**
   *  从 csv 读取，然后写入 csv
   *  from_unixtime bigint 转换为 varchar 时间
   *  to_timestamp varchar 时间转换为时间戳
   */
  @Test
  def csv(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

//    tabEnv.sqlQuery(
//      """
//        |select from_unixtime(1547718202) as t
//        |""".stripMargin
//    ).execute()
//      .print()
    // from
    tabEnv.executeSql(
      s"""
        |create table sensor (
        | id string,
        | tmstp bigint,
        | amount int,
        | rowtime as to_timestamp(from_unixtime(tmstp))
        |) with (
        | 'connector'='filesystem',
        | 'path'='${getResourcePath("sensor/sensor.txt")}',
        | 'format'='csv'
        |)
        |""".stripMargin
    )

    tabEnv.executeSql(
      s"""
         |create table sensor2 (
         | id string,
         | amount int,
         | rowtime timestamp(3)
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("output/sensor")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )

    tabEnv.executeSql(
      """
        |insert into sensor2 select id,amount,rowtime from sensor
        |""".stripMargin
    ).await()
  }

  /**
   * {"id":"sensor_1","amount":10}
   * 读取 kafka 消息同时，抓取消息相关元数据，如：消息接收时间、消息偏移量信息。
   */
  @Test
  def metadata(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    // 直接将 kafka 中消息的接收时间作为 rowtime
    tabEnv.executeSql(
      """
        |create table sensor1 (
        | id string,
        | amount bigint,
        | rowtime timestamp(3) with local time zone metadata from 'timestamp'
        |) with (
        |  'connector' = 'kafka',
        |  'topic' = 'sensor',
        |  'properties.group.id'='flink-group',
        |  'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
        |  'format' = 'json',
        |  'scan.startup.mode' = 'earliest-offset'
        |)
        |""".stripMargin)

//    tabEnv.sqlQuery("select * from sensor1")
//      .execute()
//      .print()

    // 直接想 kafka 要消息的元数据信息 timestamp，并基于它衍生出 rowtime
    // 并提取消息的 offset 信息
    tabEnv.executeSql(
      """
        |create table sensor2 (
        |  id string,
        |  amount bigint,
        | `partition` BIGINT METADATA VIRTUAL,  -- 分区，虚拟列
        | `offset` BIGINT METADATA VIRTUAL, -- 分区偏移量
        | `topic` STRING METADATA VIRTUAL, -- 主题
        | `timestamp` TIMESTAMP(3) metadata, -- 元数据列，消息时间戳
        | tmstp as cast(`timestamp` as bigint), -- 计算列
        | watermark for `timestamp` as `timestamp` - interval '5' second -- 水印列
        |) with (
        |  'connector' = 'kafka',
        |  'topic' = 'sensor',
        |  'properties.group.id'='flink-group2',
        |  'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
        |  'format' = 'json',
        |  'scan.startup.mode' = 'earliest-offset'
        |)
        |""".stripMargin)

    tabEnv.sqlQuery("select * from sensor2")
      .execute()
      .print()
  }

  /**
   * 基于已有字段衍生出新字段
   */
  @Test
  def computeColumn(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    // 直接将 kafka 中消息的接收时间作为 rowtime
    tabEnv.executeSql(
      """
        |create table orders (
        | id string,
        | amount bigint,
        | price double,
        | sales as amount * price -- 计算列
        |) with (
        |  'connector' = 'kafka',
        |  'topic' = 'orders',
        |  'properties.group.id'='flink-group',
        |  'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
        |  'format' = 'json',
        |  'scan.startup.mode' = 'earliest-offset'
        |)
        |""".stripMargin)

    tabEnv.sqlQuery("select * from orders")
      .execute()
      .print()
  }

  /**
   * 严格自增水印 watermark for rowtime as rowtime
   * 非严格自增水印：watermark for rowtime as rowtime - interval '0.05' second
   * 乱序水印：watermark for rowtime as rowtime - interval '5' second
   **/
  @Test
  def watermarkColumn(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    // 直接将 kafka 中消息的接收时间作为 rowtime
    tabEnv.executeSql(
      """
        |create table orders (
        | id string,
        | amount bigint,
        | price double,
        | sales as amount * price, -- 计算列
        | rowtime timestamp(3) metadata from 'timestamp',
        | watermark for rowtime as rowtime - interval '30' second
        |) with (
        |  'connector' = 'kafka',
        |  'topic' = 'orders',
        |  'properties.group.id'='flink-group',
        |  'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
        |  'format' = 'json',
        |  'scan.startup.mode' = 'earliest-offset'
        |)
        |""".stripMargin)

    tabEnv.sqlQuery("select * from orders")
      .execute()
      .print()
  }

  /**
   * primary key 需要是不为 null 的，且需要保证唯一性，唯一性约束需要用户自己保证
   */
  @Test
  def primaryKey(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    // 直接将 kafka 中消息的接收时间作为 rowtime
    tabEnv.executeSql(
      """
        |create table orders (
        | id string,
        | amount bigint,
        | price double,
        | sales as amount * price, -- 计算列
        | rowtime timestamp(3) metadata from 'timestamp',
        | watermark for rowtime as rowtime - interval '30' second
        |) with (
        |  'connector' = 'kafka',
        |  'topic' = 'orders',
        |  'properties.group.id'='flink-group',
        |  'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
        |  'format' = 'json',
        |  'scan.startup.mode' = 'earliest-offset'
        |)
        |""".stripMargin)

    tabEnv.executeSql(
      """
        |create table orders_result (
        | id string primary key not enforced, -- 不强制检查，要程序员自己控制主键唯一性
        | amount bigint,
        | price double,
        | sales double, -- 计算列
        | timestamp timestamp(3)
        |) with (
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

    tabEnv.sqlQuery("insert into orders_result select id,amount,price,sales,timestamp from orders")
      .execute()
      .print()
  }

  /**
   * partitioned by 基于给定字段进行分区，体现在文件系统落盘时，基于分区创建不同目录
   */
  @Test
  def partitionedBy(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      s"""
         |create table sensor (
         | id string,
         | tmstp bigint,
         | amount int,
         | rowtime as to_timestamp(from_unixtime(tmstp))
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )

    tabEnv.executeSql(
      s"""
         |create table sensor2 (
         | id string,
         | amount int,
         | rowtime timestamp(3),
         | dt string
         |) partitioned by (dt) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("output/sensor")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )

//    tabEnv.sqlQuery("select id,amount,from_unixtime(cast(rowtime as bigint),'yyyyMMdd') as dt from sensor")
//      .execute()
//      .print()

    tabEnv.executeSql(
      """
        |insert into sensor2(id,amount,rowtime,dt) select id,amount,rowtime,from_unixtime(cast(rowtime as bigint),'yyyyMMdd') as dt from sensor
        |""".stripMargin
    ).await()
  }

  @Test
  def like(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      s"""
         |create table sensor1 (
         | id string,
         | tmstp bigint,
         | amount int,
         | rowtime as to_timestamp(from_unixtime(tmstp))
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )
    // 完全照搬
    tabEnv.executeSql(
      s"""
         |create table sensor2 like sensor1
         |""".stripMargin
    )

//    tabEnv.sqlQuery("select * from sensor2")
//      .execute()
//      .print()

    // 从 sensor1 的普通列 tmstp 衍生出 time1，并给予此产生水印
    tabEnv.executeSql(
      s"""
         |create table sensor3(
         |  time1 as to_timestamp(from_unixtime(tmstp)),
         |  watermark for time1 as time1 - interval '5' second
         |) like sensor1
         |""".stripMargin
    )

//    tabEnv.sqlQuery("select * from sensor3")
//      .execute()
//      .print()

    tabEnv.executeSql(
      s"""
         |create table sensor4 (
         |  time1 as to_timestamp(from_unixtime(tmstp)),
         |  watermark for time1 as time1 - interval '5' second
         |) with (
         |  'connector'='filesystem',
         |  'path'='${getResourcePath("sensor/sensor.json")}', --- 重写
         |  'format'='json'
         |) like sensor1 (
         |  including generated -- 包含 入 rowtime
         |)
         |""".stripMargin
    )

//    tabEnv.sqlQuery("select * from sensor4")
//      .execute()
//      .print()


    tabEnv.executeSql(
      s"""
         |create table sensor5 (
         |  t1 timestamp(3),
         |  watermark for t1 as t1 - interval '5' second
         |) with (
         |  'connector'='filesystem',
         |  'path'='${getResourcePath("sensor/sensor2.json")}', --- 重写
         |  'format'='json'
         |) like sensor1 (
         |  EXCLUDING ALL -- 据说是 忽略所有普通列，貌似没有用
         |  INCLUDING GENERATED -- 包含 入 rowtime
         |)
         |""".stripMargin
    )
    tabEnv.sqlQuery("select * from sensor5")
      .execute()
      .print()
  }

  /**
   * 默认是 default_catalog 的 default_database
   * AbstractCatalog (org.apache.flink.table.catalog)
   * AbstractJdbcCatalog、GenericInMemoryCatalog、HiveCatalog
   *
   */
  @Test
  def createCatelog(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val catalogName1 = "mycatalog"

    val database1Name = "mydb"
    val props = new java.util.HashMap[String,String]();
    val db = new CatalogDatabaseImpl(props,"comment ...")

    val catalog = new GenericInMemoryCatalog(catalogName1)
    catalog.createDatabase(database1Name,db,false)

    tabEnv.registerCatalog(catalogName1,catalog)

//    tabEnv.executeSql("create catalog mycatalog2") // 没有这种创建方式

    println(tabEnv.listCatalogs().mkString(",")) // default_catalog,mycatalog
    println(tabEnv.listDatabases().mkString(",")) // default_database

    tabEnv.useCatalog(catalogName1)
    println(tabEnv.listDatabases().mkString(",")) // default,mydb

    println(tabEnv.getCatalog(catalogName1)) // mycatalog
    println(tabEnv.getCurrentCatalog) // mycatalog
  }

  /**
   * hive catalog 除了可以访问hive数据外，更重要的是可操作元数据（建表、改表、新建分区等）
   * 使用前需要启动metastore (bin/metaStart)
   * nohup hive --service metastore >/dev/null 2>&1 &
   * lsof -i :9083
   */
  @Test
  def hiveCatalog(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val catalogName="hiveCatalog"
    val defaultDatabase = "default"
    val hiveConfigDir = getResourcePath("hive") // 自己补全  hive-site.xml
    val catalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfigDir)

    tabEnv.registerCatalog(catalogName,catalog)

    tabEnv.useCatalog(catalogName)
    tabEnv.useDatabase(defaultDatabase)

    tabEnv.executeSql(" select * from log_orc limit 5").print()
  }


  @Test
  def createDatabase(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.executeSql("create database mydb1") // 直接在默认default_catalog上创建db

    val catalogName1 = "mycatalog"
    val database1Name = "mydb2"
    val props = new java.util.HashMap[String,String]();
    val db = new CatalogDatabaseImpl(props,"comment ...")
    val catalog = new GenericInMemoryCatalog(catalogName1)
    catalog.createDatabase(database1Name,db,false) // 创建内置db的catalog，然后再注册
    tabEnv.registerCatalog(catalogName1,catalog)

    tabEnv.executeSql("create database if not exists mycatalog.mydb3")

    println(tabEnv.listDatabases().mkString(",")) // default_database,mydb1

    tabEnv.useCatalog(catalogName1)
    println(tabEnv.listDatabases().mkString(",")) // default,mydb2,mydb3
  }

  @Test
  def createView(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql("create temporary view if not exists now_view as select now() as n")

    tabEnv.sqlQuery("select * from now_view")
      .execute()
      .print()

    val ds = env.fromElements((1,2,3),(10,20,30))
    tabEnv.createTemporaryView("tab",ds,'a,'b,'c)
    tabEnv.sqlQuery("select * from tab")
      .execute()
      .print()

  }

  @Test
  def createFunction(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.createFunction("substr",classOf[SubStr])
    tabEnv.sqlQuery("select ar.name1,ar.name2 from (select substr('abcd2009') ar) temp")
      .execute()
      .print()
  }

  @Test
  def dropTable(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.executeSql(
      s"""
         |create table sensor1 (
         | id string,
         | tmstp bigint,
         | amount int,
         | rowtime as to_timestamp(from_unixtime(tmstp))
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )

    tabEnv.executeSql(
      s"""
         |create table sensor2 like sensor1
         |""".stripMargin
    )

//    tabEnv.dropTemporaryTable("sensor2")

    println(tabEnv.listTables().mkString(",")) // sensor1,sensor2

    tabEnv.executeSql("drop table if exists sensor1")
    println(tabEnv.listTables().mkString(",")) // sensor2

    tabEnv.executeSql("show tables").print() // sensor2
  }

  @Test
  def dropDatabase(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    //
    tabEnv.executeSql("create database mydb")

    tabEnv.executeSql(
      s"""
         |create table mydb.sensor1 (
         | id string,
         | tmstp bigint,
         | amount int,
         | rowtime as to_timestamp(from_unixtime(tmstp))
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )
    // 默认 restrict 既如果为空库，就可以删除，否则报错不能删除，默认
    // cascade 即便不为空也要删除
    tabEnv.executeSql("drop database if exists mydb cascade")
    println(tabEnv.listDatabases().mkString(","))
  }

  @Test
  def dropView(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      """
        |create temporary view my_now as select now() as n
        |""".stripMargin
    )

    tabEnv.sqlQuery("select * from my_now")
      .execute()
      .print()

    println(tabEnv.listViews().mkString(","))

    tabEnv.dropTemporaryView("my_now")
    tabEnv.executeSql("drop view if exists my_now")
  }

  @Test
  def dropFunction(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.createFunction("substr",classOf[SubStr]) // 指定catalog 下注册永久函数
    tabEnv.createTemporaryFunction("substr",classOf[SubStr]) // 指定 catalog 下注册临时函数
    tabEnv.createTemporarySystemFunction("substr",classOf[SubStr]) // 注册系统函数，无catalog概念，所有catalog都可以用

    tabEnv.executeSql("create database mydb")

    tabEnv.executeSql(
      s"""
         |create table mydb.sensor1 (
         | id string,
         | tmstp bigint,
         | amount int,
         | rowtime as to_timestamp(from_unixtime(tmstp))
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )

    tabEnv.sqlQuery("select ar.name1,ar.name2 from (select substr('abcd2009') ar) temp")
      .execute()
      .print()

//    tabEnv.dropFunction("substr")
//    tabEnv.dropTemporaryFunction("substr")
//    tabEnv.dropTemporarySystemFunction("substr")
  }

  @Test
  def alterTable(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      s"""
         |create table sensor1 (
         | id string,
         | tmstp bigint,
         | amount int,
         | rowtime as to_timestamp(from_unixtime(tmstp))
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )

    // 表重命名
    tabEnv.executeSql("alter table sensor1 rename to sensor2")
//    tabEnv.sqlQuery("select * from sensor2")
//      .execute()
//      .print()

    // 重新设置表的 kv 配置
    tabEnv.executeSql(s"alter table sensor2 set ('path'='${getResourcePath("sensor/sensor2.txt")}')")

    tabEnv.sqlQuery("select * from sensor2")
      .execute()
      .print()


    tabEnv.executeSql("create database mydb")

    /**
     * 暂无运用示例
     * alter database if exists mydb set ('key'='value',...)
     * alter function if exists xxx as ...
     */
  }

  @Test
  def insert(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.executeSql(
      s"""
         |create table data (
         | `user` string,
         | cnt int,
         | `date` string,
         | country string
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("input/country/page.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )

    tabEnv.executeSql(
      s"""
         |create table country_page_view (
         | `user` string,
         | cnt int,
         | `date` string,
         | country string
         |) partitioned by (`date`,country) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("output/country")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )

    // 直接基于动态分区插入
//    tabEnv.executeSql("insert into country_page_view select * from data")
//      .await()


//    tabEnv.sqlQuery("select * from data where `date`='20210101' and country='US'").execute().print()

    // 插入指定分区
    val result = tabEnv.executeSql(
      """insert into country_page_view partition(`date`='20210101',country='US')
        |select user,cnt from data where `date`='20210101' and country='US'
        |""".stripMargin
    ).await()

  }

  @Test
  def hiveInsert(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    env.enableCheckpointing(10000)
    // {"user_id":101,"order_amount":10}
    // ts timestamp(3) with local time zone metadata from 'timestamp',
//    tabEnv.executeSql(
//      """
//        |create table country_pv (
//        | user_id string,
//        | order_amount bigint,
//        | ts timestamp(3) with local time zone metadata from 'timestamp',
//        | `dt` as DATE_FORMAT(ts,'yyyyMMdd')
//        |) with (
//        |  'connector' = 'kafka',
//        |  'topic' = 'country_pv',
//        |  'properties.group.id'='flink-group',
//        |  'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
//        |  'format' = 'json',
//        |  'scan.startup.mode' = 'earliest-offset'
//        |)
//        |""".stripMargin)

//    tabEnv.sqlQuery("select * from country_pv").execute().print()

    val ds = env.addSource(new OrderSource()).assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner[OrderInfo]{
        override def extractTimestamp(t: OrderInfo, l: Long): Long = t.ts.getTime
      }
    ))

//    ds.print()
    tabEnv.createTemporaryView("country_pv",ds)

//    tabEnv.sqlQuery("select * from country_pv").execute().print()

    val catalogName = "hive_catalog"
    val hiveConfDir = getResourcePath("hive")
    val hiveDB = "default"

    val hiveCatalog = new HiveCatalog(catalogName, hiveDB, hiveConfDir, null);

    tabEnv.getConfig.setLocalTimeZone(ZoneId.of("UTC"))

    tabEnv.registerCatalog(catalogName,hiveCatalog)
    tabEnv.useCatalog(catalogName)
    tabEnv.useDatabase(hiveDB)
    tabEnv.getConfig.setSqlDialect(SqlDialect.HIVE)

    // kafka 流式数据 借助 processtime 写入 hive
    // hive 表如果不存在，就执行新建
    tabEnv.executeSql(
      """
        |CREATE EXTERNAL TABLE IF NOT EXISTS `fs_table`(
        |  `user_id` string,
        |  `order_amount` bigint
        |) PARTITIONED BY (
        |  `dt` string,
        |  `h` string,
        |  `m` string)
        |stored as ORC
        |TBLPROPERTIES (
        |  'sink.partition-commit.policy.kind'='metastore',
        |  'partition.time-extractor.timestamp-pattern'='$dt $h:$m:00',
        |  'sink.partition-commit.trigger'='partition-time',
        |  'sink.partition-commit.delay'='0s'
        |)
        |""".stripMargin)

    tabEnv.sqlQuery(
      """SELECT
        | user_id,
        | order_amount,
        | DATE_FORMAT(ts, 'yyyy-MM-dd') as dt,
        | DATE_FORMAT(ts, 'HH') as h,
        | DATE_FORMAT(ts, 'mm') as m
        |FROM default_catalog.default_database.country_pv
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.executeSql(
      """insert into fs_table
        |SELECT
        | user_id,
        | order_amount,
        | DATE_FORMAT(ts, 'yyyy-MM-dd') as dt,
        | DATE_FORMAT(ts, 'HH') as h,
        | DATE_FORMAT(ts, 'mm') as m
        |FROM default_catalog.default_database.country_pv
        |""".stripMargin
    ).await()

    // 时区问题导致 无法正常提交分区，但数据时写成功的
  }

  // sql 增强提示
  @Test
  def sqlHint(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    // 允许动态覆盖表属性
    tabEnv.getConfig.getConfiguration.setString("table.dynamic-table-options.enabled","true");
//    configuration.setString("table.exec.mini-batch.enabled", "true")
//    configuration.setString("table.exec.mini-batch.allow-latency", "5 s")
//    configuration.setString("table.exec.mini-batch.size", "5000")
    tabEnv.executeSql(
      s"""
         |create table sensor (
         | id string,
         | tmstp bigint,
         | amount int,
         | rowtime as to_timestamp(from_unixtime(tmstp))
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )

    // 动态替换 path
//    tabEnv.sqlQuery(s"select * from sensor /*+ OPTIONS('path'='${getResourcePath("sensor/sensor2.txt")}') */")
//      .execute()
//      .print()

    tabEnv.executeSql(
      """
        |create table country_pv (
        | user_id string,
        | order_amount bigint,
        | ts timestamp(3) with local time zone metadata from 'timestamp',
        | `dt` as DATE_FORMAT(ts,'yyyyMMdd')
        |) with (
        |  'connector' = 'kafka',
        |  'topic' = 'country_pv',
        |  'properties.group.id'='flink-group',
        |  'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
        |  'format' = 'json',
        |  'scan.startup.mode' = 'earliest-offset'
        |)
        |""".stripMargin)

    // 从最新的开始消费
//    tabEnv.sqlQuery(s"select * from country_pv /*+ OPTIONS('scan.startup.mode'='latest-offset') */")
//      .execute()
//      .print()

    tabEnv.executeSql(
      """
        |create table country_pv2 (
        | user_id string,
        | order_amount bigint
        |) with (
        |  'connector' = 'kafka',
        |  'topic' = 'country_pv2',
        |  'properties.group.id'='flink-group',
        |  'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
        |  'format' = 'json'
        |)
        |""".stripMargin
    )

    // 轮询输出 kafka -> kafka
    tabEnv.executeSql(
      """
        |insert into country_pv2 /*+ OPTIONS('sink.partitioner'='round-robin') */
        |select user_id,order_amount from country_pv
        |""".stripMargin
    ).await()
  }

  /**
   *  +--------------+-----------------------------------+------+-----+----------------------------------+-----------+
      |         name |                              type | null | key |                           extras | watermark |
      +--------------+-----------------------------------+------+-----+----------------------------------+-----------+
      |      user_id |                            STRING | true |     |                                  |           |
      | order_amount |                            BIGINT | true |     |                                  |           |
      |           ts | TIMESTAMP(3) WITH LOCAL TIME ZONE | true |     |        METADATA FROM 'timestamp' |           |
      |           dt |                            STRING | true |     | AS DATE_FORMAT(`ts`, 'yyyyMMdd') |           |
      +--------------+-----------------------------------+------+-----+----------------------------------+-----------+
   */
  @Test
  def describe(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      """
        |create table country_pv (
        | user_id string,
        | order_amount bigint,
        | ts timestamp(3) with local time zone metadata from 'timestamp',
        | `dt` as DATE_FORMAT(ts,'yyyyMMdd')
        |) with (
        |  'connector' = 'kafka',
        |  'topic' = 'country_pv',
        |  'properties.group.id'='flink-group',
        |  'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
        |  'format' = 'json',
        |  'scan.startup.mode' = 'earliest-offset'
        |)
        |""".stripMargin)

    tabEnv.executeSql("describe country_pv").print()

  }

  @Test
  def explain(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      s"""
         |create table sensor1 (
         | id string,
         | tmstp bigint,
         | amount int,
         | rowtime as to_timestamp(from_unixtime(tmstp))
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )

    tabEnv.executeSql(
      s"""
         |create table sensor2 (
         | id string,
         | tmstp bigint,
         | amount int,
         | rowtime as to_timestamp(from_unixtime(tmstp))
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor2.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )
    // 物理执行计划 explain plan for
    tabEnv.executeSql(
      """
        |explain plan for
        |select t1.id,t1.amount,t1.tmstp from
        |sensor1 as t1
        |join
        |sensor2 as t2
        | using(id)
        |""".stripMargin
    ).print()

  }


  @Test
  def use(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.executeSql("create database mydb1") // 直接在默认default_catalog上创建db

    val catalogName1 = "mycatalog"
    val database1Name = "mydb2"
    val props = new java.util.HashMap[String,String]();
    val db = new CatalogDatabaseImpl(props,"comment ...")
    val catalog = new GenericInMemoryCatalog(catalogName1)
    catalog.createDatabase(database1Name,db,false) // 创建内置db的catalog，然后再注册
    tabEnv.registerCatalog(catalogName1,catalog)

    // 声明默认 catalog 和 db
    tabEnv.executeSql("use catalog mycatalog")
    tabEnv.executeSql("use mycatalog.mydb2")

    tabEnv.executeSql(
      s"""
         |create table sensor1 (
         | id string,
         | tmstp bigint,
         | amount int,
         | rowtime as to_timestamp(from_unixtime(tmstp))
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin
    )

    tabEnv.executeSql(
      """
        |create temporary view my_now as select now() as n
        |""".stripMargin
    )

    tabEnv.createFunction("substr",classOf[SubStr])

    tabEnv.executeSql("show catalogs").print()
    tabEnv.executeSql("show current catalog").print()

    tabEnv.executeSql("show databases").print()
    tabEnv.executeSql("show current database").print() // 不支持 SHOW CREATE DATABASE

    tabEnv.executeSql("show tables").print()
    tabEnv.executeSql("describe sensor1").print() // 不支持show create table

    tabEnv.executeSql("show views").print()
    tabEnv.executeSql("describe my_now").print()

    tabEnv.executeSql("show functions").print() // 无描述

  }



}

@FunctionHint(output=new DataTypeHint("Row<name1 STRING,name2 STRING>"))
class SubStr extends ScalarFunction {

  def eval(input:String): Row = {
    Row.of(input.substring(0,4),input.substring(4,8))
  }
  // 有函数注解就可以省略 返回值类型
  //  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
  //    Types.ROW(Types.STRING(),Types.STRING())
  //  }

}

case class OrderInfo(user_id:String,order_amount:Int,ts:Timestamp)
class OrderSource extends SourceFunction[OrderInfo]{

  private var isRunning = true

  private val random = new Random()

  private val user_ids = Array(
    "4760858d-2bec-483c-a535-291de04b2247", "67088699-d4f4-43f2-913c-481bff8a2dc5",
    "72f7b6a8-e1a9-49b4-9a0b-770c41e01bfb", "dfa27cb6-bd94-4bc0-a90b-f7beeb9faa8b",
    "aabbaa50-72f4-495c-b3a1-70383ee9d6a4", "3218bbb9-5874-4d37-a82d-3e35e52d1702",
    "3ebfb9602ac07779||3ebfe9612a007979", "aec20d52-c2eb-4436-b121-c29ad4097f6c",
    "e7e896cd939685d7||e7e8e6c1930689d7", "a4b1e1db-55ef-4d9d-b9d2-18393c5f59ee",
  )

  override def run(sourceContext: SourceFunction.SourceContext[OrderInfo]): Unit = {
    while (isRunning) {
      val user_id = user_ids((Math.random * (user_ids.length - 1)).asInstanceOf[Int])
      val order_amount = random.nextInt(100)
      val ts = new Timestamp(System.currentTimeMillis)
      val userInfo = new OrderInfo(user_id,order_amount,ts)
      sourceContext.collect(userInfo)
      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}



