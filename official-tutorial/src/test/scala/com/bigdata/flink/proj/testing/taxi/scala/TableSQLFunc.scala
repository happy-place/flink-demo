package com.bigdata.flink.proj.testing.taxi.scala

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigdata.flink.proj.common.func.CommonUtil.getResourcePath
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



class TableSQLFunc {

  /**
   * TableEnvironment.create(settings)
   * TabEnv 功能:
   * 1.注册表到内部catalogs
   * 2.注册外部catalog
   * 3.加载可插拔组件
   * 4.执行sql查询
   * 5.注册自定义(标量、聚合)函数
   * 6.控制流和表之间的转换
   *
   *
   */
  @Test
  def envCreate1(): Unit = {
    val settings = EnvironmentSettings.newInstance()
      .inBatchMode()
      .build()
    val tabEnv = TableEnvironment.create(settings)

    tabEnv.executeSql(
      s"""create table sensor(
         | id string,
         | ts bigint,
         | vc int,
         | pt as PROCTIME()
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    val sql = "select id,ts,vc,pt from sensor"
    tabEnv.executeSql(sql).print()

    // TableResult才能打印
    //    tabEnv.sqlQuery(sql).execute().print()
  }

  /**
   * StreamTableEnvironment.create(env)
   */
  @Test
  def envCreate2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      s"""create table sensor(
         | id string,
         | ts bigint,
         | vc int,
         | pt as PROCTIME()
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    // TableResult 直接打印 表格
    tabEnv.executeSql("select id,ts,vc,pt from sensor").print()

    // Table 需要借助 table2RowDataStream 才能打印输出记录
    //    val table = tabEnv.sqlQuery("select id,ts,vc,pt from sensor")
    //    table.print()
    //    env.execute("envCreate2")
  }

  /**
   * 1.自定义catalog 时，catalog 和 catalog 的默认库需要显示注册，如果不自定义就使用系统默认的
   * 2.catalog的其他库可以直接声明使用，不需要显示注册
   * 3.已经存在的catalog,通过定义重名的catalog，可以对其实行覆盖
   * 4.需要长久保存的catalog,需要存盘保存
   *
   * catalog三要素：catalog 名称、数据库名称、存储对象名称
   *
   */
  @Test
  def catalog(): Unit = {

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tabEnv = TableEnvironment.create(settings)

    val my_catalog = new GenericInMemoryCatalog("my_catalog", "my_database")
    val other_catalog = new GenericInMemoryCatalog("other_catalog", "default")

    tabEnv.registerCatalog("my_catalog", my_catalog)
    tabEnv.registerCatalog("other_catalog", other_catalog)

    // 声明使用的默认catalog database
    tabEnv.useCatalog("my_catalog")
    tabEnv.useDatabase("my_database")

    // 在默认 my_catalog.my_database 下注册表 sensor
    tabEnv.executeSql(
      s"""create table sensor(
         | id string,
         | ts bigint,
         | vc int,
         | pt as PROCTIME()
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    /**
     * 等效于，不带 catalog、database 前缀时，使用默认的
     * select * from my_catalog.my_database.sensor where id='sensor_7'
     */
    val table = tabEnv.sqlQuery("select * from sensor where id='sensor_7'")
    tabEnv.createTemporaryView("my_view", table)

    /**
     * 在默认catalog 上声明注册新视图到非默认库，此非默认库无需显示注册
     */
    tabEnv.createTemporaryView("other_database.my_view", table)

    /**
     * 注册新视图到非默认catalog,非默认库
     */
    tabEnv.createTemporaryView("other_catalog.other_database.my_view", table)

    //    tabEnv.sqlQuery("select * from my_catalog.my_database.my_view").execute().print()
    tabEnv.sqlQuery("select * from my_catalog.other_database.my_view").execute().print()
    //    tabEnv.sqlQuery("select * from other_catalog.other_database.my_view").execute().print()

  }

  @Test
  def expression(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.executeSql(
      s"""create table sensor(
         | id string,
         | ts bigint,
         | vc int,
         | pt as PROCTIME()
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    tabEnv.from("sensor")
      //      .where($"id" ===  "sensor_7") // 需要添加 org.apache.flink.table.api._
      .where("id = 'sensor_7'") // 需要添加 org.apache.flink.table.api._
      //      .select($"id",$"ts",$"vc",$"pt")
      .select("id,ts,vc,pt")
      .execute()
      .print()
  }

  /**
   * +----+--------------------------------+-------------+
   * | op |                             id |      EXPR$1 |
   * +----+--------------------------------+-------------+
   * | +I |                       sensor_7 |          38 |
   * | -U |                       sensor_7 |          38 |  <- 撤销之前的
   * | +U |                       sensor_7 |          25 |  <- 新平均值
   * | +I |                       sensor_1 |          35 |
   * | +I |                       sensor_6 |          12 |
   * +----+--------------------------------+-------------+
   */
  @Test
  def aggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.executeSql(
      s"""create table sensor(
         | id string,
         | ts bigint,
         | vc int,
         | pt as PROCTIME()
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    // 聚合，每进入一个元素就迭代计算异常，
    tabEnv.executeSql(
      """select
        | id
        | ,avg(vc) as avg_vc
        |from sensor
        |group by id
        |""".stripMargin
    ).print()
  }

  /**
   * jdbc-connector 配置 （https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/connectors/table/jdbc/）
   * 必须使用依赖，否则会出现 default_catalog、default_database 中找不到 mysql sink 表的报错，无从查找错误原因
   * <dependency>
   * <groupId>org.apache.flink</groupId>
   * <artifactId>flink-connector-jdbc_${scala.major}</artifactId>
   * <version>${flink.version}</version>
   * </dependency>
   *
   * <dependency>
   * <groupId>mysql</groupId>
   * <artifactId>mysql-connector-java</artifactId>
   * <version>${mysql.version}</version>
   * </dependency>
   *
   * BatchTableSink: for batch tabl;
   * AppendStreamTableSink: for MQ
   * RetractStreamTableSink、UpsertStreamTableSink：for database which has a primary key
   * DynamicTableSink: 自己实现动态表sink https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/sourcessinks/
   */
  @Test
  def insert1(): Unit = {
    // 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    // 注册 source
    tabEnv.executeSql(
      s"""create table sensor(
         | id string,
         | ts bigint,
         | vc int,
         | pt as PROCTIME()
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    // 注册 sink， jdbc-connector 需要设置自动提交，有主键才支持upsert，否则只支持append
    /**
     * CREATE TABLE sensor_avg (
     * id varchar(10)
     * ,avg_vc int(3)
     * ,primary key(id)
     * );
     */
    tabEnv.executeSql(
      s"""CREATE TABLE sensor_avg (
         |  id string
         |  ,avg_vc int
         |  ,PRIMARY KEY (id) NOT ENFORCED
         |) WITH (
         |   'connector' = 'jdbc',
         |   'url' = 'jdbc:mysql://hadoop01:3306/test',
         |   'driver' = 'com.mysql.cj.jdbc.Driver',
         |    'scan.auto-commit' = 'true',
         |   'table-name' = 'sensor_avg',
         |   'username' = 'test',
         |   'password' = 'test'
         |)
         |""".stripMargin)

    // 支持sink 插入，且必须有触发操作
    // 聚合，每进入一个元素就迭代计算异常，
    tabEnv.executeSql(
      """insert into sensor_avg
        |select
        | id
        | ,avg(vc) as avg_vc
        |from sensor
        |group by id
        |""".stripMargin
    ).await() // 必须要有print 或 await 执行算子才能保证正常执行成功

    //    tabEnv.executeSql("select * from sensor_avg").print()
  }

  @Test
  def insert2(): Unit = {
    // 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    // 注册 source
    tabEnv.executeSql(
      s"""create table sensor(
         | id string,
         | ts bigint,
         | vc int,
         | pt as PROCTIME()
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    tabEnv.executeSql(
      s"""CREATE TABLE sensor_avg (
         |  id string
         |  ,avg_vc int
         |  ,PRIMARY KEY (id) NOT ENFORCED
         |) WITH (
         |   'connector' = 'jdbc',
         |   'url' = 'jdbc:mysql://hadoop01:3306/test',
         |   'driver' = 'com.mysql.cj.jdbc.Driver',
         |   'table-name' = 'sensor_avg',
         |   'username' = 'test',
         |   'password' = 'test'
         |)
         |""".stripMargin)

    val statementSet = tabEnv.createStatementSet()
    val tab = tabEnv.sqlQuery(
      """select
        | id
        | ,avg(vc) as avg_vc
        |from sensor
        |group by id
        |""".stripMargin
    )
    statementSet.addInsert("sensor_avg", tab)
    statementSet.execute().await() // 必须 await 或 print 才能出发执行
  }

  /**
   * 不使用 sql 声明式，使用编码式构建 source table
   */
  @Test
  def csvSchema(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val schema = new Schema().field("id", DataTypes.STRING())
      .field("ts", DataTypes.BIGINT())
      .field("vc", DataTypes.INT())

    tabEnv.connect(new FileSystem().path(getResourcePath("sensor/sensor.txt")))
      .withFormat(new Csv().fieldDelimiter(',').deriveSchema()) // 如何解析文件
      .withSchema(schema) // 给解析后的tuple 附上怎样字段描述
      .inAppendMode() // 数据生成模式
      .createTemporaryTable("sensor") // 注册为动态表

    tabEnv.sqlQuery("select * from sensor where id='sensor_7'").execute().print()

  }

  /**
   * 查询优化，多数基于规则和开销进行优化
   * Subquery decorrelation based on Apache Calcite  子查询关系装饰
   * Project pruning                                 字段裁剪
   * Partition pruning                               分区裁剪
   * Filter push-down                                filter 下推
   * Sub-plan deduplication to avoid duplicate computation 子查询去重(重复直接引用)
   * Special subquery rewriting, including two parts:      针对性重写优化
   * Converts IN and EXISTS into left semi-joins         in exists 替换为 left semi join
   * Converts NOT IN and NOT EXISTS into left anti-join  not in、not exists 替换为 left anti join
   * Optional join reordering                              join 顺序调整优化（小表驱动大表）
   * Enabled via table.optimizer.join-reorder-enabled    需要开启配置：table.optimizer.join-reorder-enabled
   */


  /**
   * 抽象语法树（优化前的执行计划）
   * == Abstract Syntax Tree ==
   * LogicalUnion(all=[true])
   * :- LogicalFilter(condition=[LIKE($1, _UTF-16LE'f%')])
   * :  +- LogicalTableScan(table=[[Unregistered_DataStream_1]])
   * +- LogicalTableScan(table=[[Unregistered_DataStream_2]])
   *
   * 优化后的逻辑计划
   * == Optimized Logical Plan ==
   * Union(all=[true], union=[count, word])
   * :- Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'f%')])
   * :  +- DataStreamScan(table=[[Unregistered_DataStream_1]], fields=[count, word])
   * +- DataStreamScan(table=[[Unregistered_DataStream_2]], fields=[count, word])
   *
   * 物理执行几乎
   * == Physical Execution Plan ==
   * Stage 1 : Data Source
   * content : Source: Collection Source
   *
   * Stage 2 : Data Source
   * content : Source: Collection Source
   *
   * Stage 3 : Operator
   * content : SourceConversion(table=[Unregistered_DataStream_1], fields=[count, word])
   * ship_strategy : FORWARD 转发策略 (本地上游发往本地下游)
   *
   * Stage 4 : Operator
   * content : Calc(select=[count, word], where=[(word LIKE _UTF-16LE'f%')])
   * ship_strategy : FORWARD
   *
   * Stage 5 : Operator
   * content : SourceConversion(table=[Unregistered_DataStream_2], fields=[count, word])
   * ship_strategy : FORWARD
   */
  @Test
  def explain(): Unit = {
    // 注册环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    // DataStream 注册为 table
    val table1 = env.fromElements((1, "hello"), (1, "fruit")).toTable(tabEnv, $"count", $"word")
    val table2 = env.fromElements((2, "hi")).toTable(tabEnv, $"count", $"word")
    val table = table1.where($"word".like("f%")) // 过滤出 f%
      .unionAll(table2) // Table
    //      .execute() // TableResult

    println(table.explain()) // 只有 table 才能进行分析
  }

  /**
   * == Abstract Syntax Tree ==
   * LogicalLegacySink(name=[`default_catalog`.`default_database`.`MySink1`], fields=[count, word])
   * +- LogicalFilter(condition=[LIKE($1, _UTF-16LE'f%')])
   * +- LogicalTableScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]])
   *
   * LogicalLegacySink(name=[`default_catalog`.`default_database`.`MySink2`], fields=[count, word])
   * +- LogicalUnion(all=[true])
   * :- LogicalFilter(condition=[LIKE($1, _UTF-16LE'f%')])
   * :  +- LogicalTableScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]])
   * +- LogicalTableScan(table=[[default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]]])
   *
   * == Optimized Logical Plan ==
   * Calc(select=[count, word], where=[LIKE(word, _UTF-16LE'f%')], reuse_id=[1])
   * +- LegacyTableSourceScan(table=[[default_catalog, default_database, MySource1, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])
   *
   * LegacySink(name=[`default_catalog`.`default_database`.`MySink1`], fields=[count, word])
   * +- Reused(reference_id=[1])
   *
   * LegacySink(name=[`default_catalog`.`default_database`.`MySink2`], fields=[count, word])
   * +- Union(all=[true], union=[count, word])
   * :- Reused(reference_id=[1])
   * +- LegacyTableSourceScan(table=[[default_catalog, default_database, MySource2, source: [CsvTableSource(read fields: count, word)]]], fields=[count, word])
   *
   * == Physical Execution Plan ==
   * Stage 1 : Data Source
   * content : Source: Custom File source
   *
   * Stage 2 : Operator
   * content : CsvTableSource(read fields: count, word)
   * ship_strategy : REBALANCE
   *
   * Stage 3 : Operator
   * content : SourceConversion(table=[default_catalog.default_database.MySource1, source: [CsvTableSource(read fields: count, word)]], fields=[count, word])
   * ship_strategy : FORWARD
   *
   * Stage 4 : Operator
   * content : Calc(select=[count, word], where=[(word LIKE _UTF-16LE'f%')])
   * ship_strategy : FORWARD
   *
   * Stage 5 : Operator
   * content : SinkConversionToRow
   * ship_strategy : FORWARD
   *
   * Stage 6 : Operator
   * content : Map
   * ship_strategy : FORWARD
   *
   * Stage 8 : Data Source
   * content : Source: Custom File source
   *
   * Stage 9 : Operator
   * content : CsvTableSource(read fields: count, word)
   * ship_strategy : REBALANCE
   *
   * Stage 10 : Operator
   * content : SourceConversion(table=[default_catalog.default_database.MySource2, source: [CsvTableSource(read fields: count, word)]], fields=[count, word])
   * ship_strategy : FORWARD
   *
   * Stage 12 : Operator
   * content : SinkConversionToRow
   * ship_strategy : FORWARD
   *
   * Stage 13 : Operator
   * content : Map
   * ship_strategy : FORWARD
   *
   * Stage 7 : Data Sink
   * content : Sink: CsvTableSink(count, word)
   * ship_strategy : FORWARD
   *
   * Stage 14 : Data Sink
   * content : Sink: CsvTableSink(count, word)
   * ship_strategy : FORWARD
   *
   */
  @Test
  def statementSet(): Unit = {
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tabEnv = TableEnvironment.create(settings)

    val schema = new Schema().field("count", DataTypes.INT())
      .field("word", DataTypes.STRING())

    tabEnv.connect(new FileSystem().path("/source/path1"))
      .withFormat(new Csv().deriveSchema())
      .withSchema(schema)
      .createTemporaryTable("MySource1")

    tabEnv.connect(new FileSystem().path("/source/path2"))
      .withFormat(new Csv().deriveSchema())
      .withSchema(schema)
      .createTemporaryTable("MySource2")

    tabEnv.connect(new FileSystem().path("/sink/path1"))
      .withFormat(new Csv().deriveSchema())
      .withSchema(schema)
      .createTemporaryTable("MySink1")

    tabEnv.connect(new FileSystem().path("/sink/path2"))
      .withFormat(new Csv().deriveSchema())
      .withSchema(schema)
      .createTemporaryTable("MySink2")

    val statementSet1 = tabEnv.createStatementSet()

    val tab1 = tabEnv.from("MySource1").where($"word".like("f%"))
    statementSet1.addInsert("MySink1", tab1) // 先缓存语句，等到执行 execute 时在触发执行，将所有 sink 合并为一个 DAG

    val result2 = tab1.unionAll(tabEnv.from("MySource2"))
    statementSet1.addInsert("MySink2", result2)

    //    statementSet1.execute()
    println(statementSet1.explain())

  }

  /**
   * +----+--------------------------------+-------------+
   * | op |                             id |          vc |
   * +----+--------------------------------+-------------+
   * | +I |                       sensor_1 |          35 |
   * | +I |                       sensor_7 |          12 |
   * | -D |                       sensor_7 |          12 |
   * | +I |                       sensor_7 |          50 |
   * | +I |                       sensor_6 |          12 |
   * +----+--------------------------------+-------------+
   * 使用老解释器
   * 1.使用 DataSet api 处理批任务；
   * 2.为每一个 sink 生成一张 DAG;
   * 3.不支持 catalog 统计数据；
   * 4.不支持键值对配置
   */
  @Test
  def useOldPlanner(): Unit = { // flink stream query
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env, settings)

    val ds = env.readTextFile(getResourcePath("sensor/sensor.txt"))
      .map { line =>
        val arr = line.split(",")
        (arr(0), arr(1).toLong, arr(2).toInt)
      }

    val table1 = tabEnv.fromDataStream(ds).as("id", "ts", "vc") // tuple 列取别名
    val table2 = tabEnv.fromDataStream(ds, "id", "ts", "vc")
    val table3 = tabEnv.fromDataStream(ds, $"_1" as "id", $"_2" as "ts", $"_3" as "vc")
    tabEnv.createTemporaryView("sensor", table1)

    // old planer 不能基于 kv 配置 数据源
    //    tabEnv.executeSql(
    //      s"""create table sensor(
    //         | id string,
    //         | ts bigint,
    //         | vc int
    //         |) with (
    //         | 'connector'='filesystem',
    //         | 'path'='${getResourcePath("sensor/sensor.txt")}',
    //         | 'format'='csv'
    //         |)
    //         |""".stripMargin)

    tabEnv.sqlQuery("select id,sum(vc) as vc from sensor group by id").execute().print()

  }

  /**
   * +--------------------------------+-------------+
   * |                             id |          vc |
   * +--------------------------------+-------------+
   * |                       sensor_6 |          12 |
   * |                       sensor_7 |          50 |
   * |                       sensor_1 |          35 |
   * +--------------------------------+-------------+
   */
  @Test
  def useOldPlanner2(): Unit = { // flink batch query
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tabEnv = BatchTableEnvironment.create(env)

    val ds = env.readTextFile(getResourcePath("sensor/sensor.txt"))
      .map { line =>
        val arr = line.split(",")
        (arr(0), arr(1).toLong, arr(2).toInt)
      }

    val table = tabEnv.fromDataSet(ds).as("id", "ts", "vc")
    tabEnv.createTemporaryView("sensor", table)

    tabEnv.sqlQuery("select id,sum(vc) as vc from sensor group by id").execute().print()

  }

  /**
   * +----+--------------------------------+-------------+
   * | op |                             id |          vc |
   * +----+--------------------------------+-------------+
   * | +I |                       sensor_6 |          12 |
   * | +I |                       sensor_1 |          35 |
   * | +I |                       sensor_7 |          12 |
   * | -U |                       sensor_7 |          12 |  <- old planer 中是 -D
   * | +U |                       sensor_7 |          50 |  <- old planer 中是 -I
   * +----+--------------------------------+-------------+
   */
  @Test
  def useBlinkPlanner(): Unit = { // blink stream query
    // 1.12.x 默认使用 blink
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env, settings)

    // old planer 不能基于 kv 配置 数据源
    tabEnv.executeSql(
      s"""create table sensor(
         | id string,
         | ts bigint,
         | vc int
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    tabEnv.sqlQuery("select id,sum(vc) as vc from sensor group by id").execute().print()

  }

  /**
   * +--------------------------------+-------------+
   * |                             id |          vc |
   * +--------------------------------+-------------+
   * |                       sensor_1 |          35 |
   * |                       sensor_7 |          50 |
   * |                       sensor_6 |          12 |
   * +--------------------------------+-------------+
   */
  @Test
  def useBlinkPlanner2(): Unit = { // blink batch query
    // 1.12.x 默认使用 blink
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tabEnv = TableEnvironment.create(settings) // 不需要 StreamExecutionEnv

    // old planer 不能基于 kv 配置 数据源
    tabEnv.executeSql(
      s"""create table sensor(
         | id string,
         | ts bigint,
         | vc int
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    tabEnv.sqlQuery("select id,sum(vc) as vc from sensor group by id").execute().print()

  }

  /**
   * old planer 和 blink planer 不能同时存否则报错
   * 下面代码是 blink 风格
   */
  @Test
  def useAnyPlanner(): Unit = {
    val settings = EnvironmentSettings.newInstance().useAnyPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env, settings)

    // old planer 不能基于 kv 配置 数据源
    tabEnv.executeSql(
      s"""create table sensor(
         | id string,
         | ts bigint,
         | vc int
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    tabEnv.sqlQuery("select id,sum(vc) as vc from sensor group by id").execute().print()

  }

  /**
   * 创建 table 方式
   * 1.基于已经存在表创建逻辑表（视图)，此逻辑查询会被缓存，重复使用；
   * 2.基于连接器，主要对接外部数据源：DB、MQ等
   *
   * 注：table 不能直接打印，执行 execute 转换为 TableResult 才能输出
   *
   * 输出：
   * 1.table 借助 TableSink 通用接口，可以写文件系统（CSV、Parquet、Avro)，写数据库存储(JDBC、HBase、Elasticsearch、Cassandra)，
   * 下消息队列（Kafka、RabbitMQ)；
   * 2.具体而言：批处理Table 只能通过 BatchTableSink 输出，而流处理 Table 可以借助 AppendTableSink、RetractTableSink、UpsertTableSink 输出
   * 3.table 一旦转换为 datastream，必须通过 env.execute() 触发执行
   */
  @Test
  def table1(): Unit = {
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build() // flink 1.12 默认使用 blink planer
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env, settings)

    // 基于 connector 创建表，等效于下面 dsl 风格 connector 创建表流程
    tabEnv.executeSql(
      s"""create table sensor(
         | id string,
         | ts bigint,
         | vc int
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    val schema = new Schema().field("id", "string")
      .field("ts", "bigint")
      .field("vc", "int")

    tabEnv.connect(new FileSystem().path(getResourcePath("sensor/sensor.txt")))
      .withFormat(new Csv().deriveSchema())
      .withSchema(schema)
      .inAppendMode()
      .createTemporaryTable("sensor2")

    // 基于已经存在表创建表（视图)
    val table = tabEnv.sqlQuery("select id,sum(vc) as vc from sensor group by id")
    val table1 = tabEnv.sqlQuery("select id,sum(vc) as vc from sensor2 group by id")

    table.execute().print()
  }

  @Test
  def connector(): Unit = {
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build() // flink 1.12 默认使用 blink planer
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env, settings)

    val schema = new Schema().field("id", "string")
      .field("ts", "bigint")
      .field("vc", "int")

    tabEnv.connect(new FileSystem().path(getResourcePath("sensor/sensor.txt")))
      .withFormat(new Csv().deriveSchema())
      .withSchema(schema)
      .inAppendMode()
      .createTemporaryTable("sensor")

    val table = tabEnv.sqlQuery("select id,sum(vc) as vc from sensor group by id")

    table.execute().print()

  }

  // 动态表转化为流暂时只支持 append-only stream、retract stream
  @Test
  def toAppendStream(): Unit = {
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env, settings)

    // old planer 不能基于 kv 配置 数据源
    tabEnv.executeSql(
      s"""create table sensor(
         | id string,
         | ts bigint,
         | vc int
         |) with (
         | 'connector'='filesystem',
         | 'path'='${getResourcePath("sensor/sensor.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    // append stream 只增不减
    val ds1 = tabEnv.sqlQuery("select * from sensor where id='sensor_7'").toAppendStream[Row]
    ds1.print("1")
    /**
     * 1:11> sensor_7,1547718205,38
     * 1:10> sensor_7,1547718202,12
     */

    // retract stream 可撤销
    val ds2 = tabEnv.sqlQuery("select id,sum(vc) as vc from sensor group by id").toRetractStream[Row]
    ds2.print("2")

    /**
     * 2:6> (true,sensor_6,12)
     * 2:3> (true,sensor_7,38)
     * 2:11> (true,sensor_1,35)
     * 2:3> (false,sensor_7,38)
     * 2:3> (true,sensor_7,50)
     */

    // table 一旦转换为 datastream，必须通过 env.execute() 触发执行
    env.execute()
  }


  /**
   * 数据库表是 insert、update、delete DML语句的 stream 结果，通常称为 changelog 流；
   * 物化视图：对应一条 sql 查询结果，结果会被缓存，随数据进入，不断执行新的查询，产生新的结果。基础表修改，缓存失效。
   *
   */

  /**
   * +----+--------------------------------+----------------------+
   * | op |                           name |               EXPR$1 |
   * +----+--------------------------------+----------------------+
   * | +I |                           Mary |                    1 |
   * | +I |                           Blob |                    1 |
   * | -U |                           Mary |                    1 | retract message
   * | +U |                           Mary |                    2 | add message
   * | +I |                            Liz |                    1 |
   * +----+--------------------------------+----------------------+
   * 不带窗口时，changelog 中有插入(insert)和更新(update)
   */
  // 连续查询
  @Test
  def continousQuery(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val seq = Seq(("Mary", "/home"),
      ("Blob", "/cart"),
      ("Mary", "/prod?id=1"),
      ("Liz", "/home"))
    val ds = env.addSource(new StreamSourceMock(seq, false))
    val clicks = tabEnv.fromDataStream(ds).as("name", "url")
    tabEnv.createTemporaryView("clicks", clicks)

    tabEnv.sqlQuery("select name,count(url) from clicks group by name")
      .execute()
      .print()
  }

  /**
   * +----+-------------------------+--------------------------------+----------------------+
   * | op |            window_start |                           name |                  cnt |
   * +----+-------------------------+--------------------------------+----------------------+
   * | +I |        2021-05-01T04:00 |                           Mary |                    3 |
   * | +I |        2021-05-01T04:00 |                            Bob |                    1 |
   * | +I |        2021-05-01T05:00 |                            Liz |                    2 |
   * | +I |        2021-05-01T05:00 |                            Bob |                    1 |
   * | +I |        2021-05-01T06:00 |                            Liz |                    1 |
   * | +I |        2021-05-01T06:00 |                           Mary |                    1 |
   * | +I |        2021-05-01T06:00 |                            Bob |                    2 |
   * +----+-------------------------+--------------------------------+----------------------+
   *
   * 带窗口统计时，changelog 中只有 insert
   *
   */
  @Test
  def continousQuery2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.getConfig.setLocalTimeZone(ZoneId.of("GMT"))
    val seq = Seq(("Mary", "2021-05-01 12:00:00", "/home"),
      ("Bob", "2021-05-01 12:00:00", "/cart"),
      ("Mary", "2021-05-01 12:02:00", "/prod?id=1"),
      ("Mary", "2021-05-01 12:55:02", "/prod?id=4"),

      ("Bob", "2021-05-01 13:01:00", "/prod?id=5"),
      ("Liz", "2021-05-01 13:30:00", "/home"),
      ("Liz", "2021-05-01 13:59:00", "/prod?id=1"),

      ("Mary", "2021-05-01 14:00:00", "/cart"),
      ("Liz", "2021-05-01 14:02:00", "/home"),
      ("Bob", "2021-05-01 14:30:00", "/prod?id=3"),
      ("Bob", "2021-05-01 14:40:00", "/home"),
    )

    // 字符串转换为 Long 型时间
    val ds = env.addSource(new StreamSourceMock(seq, false))
      .map { tup =>
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        (tup._1, sdf.parse(tup._2).getTime, tup._3)
      }.assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
        new SerializableTimestampAssigner[(String, Long, String)]() {
          override def extractTimestamp(t: (String, Long, String), l: Long): Long = t._2
        }
      )) // 声明水印

    // 声明 时间事件时间
    val clicks = tabEnv.fromDataStream(ds, $"name", $"ctime".rowtime(), $"url")
    tabEnv.createTemporaryView("clicks", clicks)

    tabEnv.sqlQuery(
      """
        |select
        | TUMBLE_START(ctime, INTERVAL '1' HOURS) as window_start
        | ,name
        | ,count(url) as cnt
        |from clicks
        |group by TUMBLE(ctime, INTERVAL '1' HOURS)
        |,name
        |
        |""".stripMargin
    ).execute()
      .print()
  }

  /**
   * 连续查询限制
   * 1.状态大小，适合累计计算，不适合憋很多数据，然后一次性触发计算，或存在数据倾斜情况的计算；
   * 2.计算更新，每次计算触发更新行有限，如果一次计算触发非常多更新，就不合适了，例如：基于最新操作时间排名
   */

  /**
   * flink 时间语义
   * 摄入时间：数据进入 flink 时间
   * 处理时间：数据被 flink 处理时间
   * 事件时间：数据自身携带时间，存在乱序可能因此通常需要使用水印和迟到容忍机制解决数据乱序问题。
   */

  /**
   * 基于官方提供 flink table connector 窗口 动态表，声明动态表字段时，单独定义一个字段 通过调用 PROCTIME()函数，接收 flink 生成的处理时间，并以此时间，作为滚动窗口操作时间
   * 注: 由于使用的是 处理时间，无乱序可能，因此默认水印就是当前处理时间，所有无需定义水印；
   */
  @Test
  def proctime1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.executeSql(
      """
        |CREATE TABLE user_actions (
        |  user_name STRING,
        |  data STRING,
        |  user_action_time AS PROCTIME() -- 直接指定一个字段作为处理时间，此时间是由 flink 产生的
        |) WITH (
        |  ... flink 支持的各种 connector,只能在 blink解析器使用，1.12.x 默认解释器就是 blink
        |);
        |""".stripMargin)

      tabEnv.sqlQuery(
        """
          |select TUMBLE_START(user_action_time,INTERVAL '10' MINUTES) as window_start
          |,count(distinct user_name) as uv
          |from user_actions
          |group by TUMBLE(user_action_time,INTERVAL '10' MINUTES)
          |""".stripMargin)
  }

  @Test
  def proctime2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // flink 1.12.x 开始默认使用 EventTime 作为时间语义，想要 flink 自己注入时间，需要使用 proctime
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val seq = Seq(("Mary", "/home"),
      ("Blob", "/cart"),
      ("Mary", "/prod?id=1"),
      ("Liz", "/home"),
    )

    val ds = env.addSource(new StreamSourceMock[(String,String)](seq,false))
    val tabEnv = StreamTableEnvironment.create(env)
    val table = tabEnv.fromDataStream(ds, $"name", $"url", $"ctime".proctime()) // 使用额外列，承接 flink 生成的 proctime

    tabEnv.createTemporaryView("clicks",table)

//    tabEnv.sqlQuery("select * from clicks").execute().print()

    // 最后一条记录，输入时窗口没有关闭，因此不会触发计算
    tabEnv.sqlQuery("""
        |select
        |tumble_start(ctime,interval '2' seconds) as window_start
        |,tumble_end(ctime,interval '2' seconds) as window_end
        |,count(distinct name) uv
        |from clicks
        |group by tumble(ctime,interval '2' seconds)
        |""".stripMargin
    ).execute()
      .print()
  }

  // 事件时间
  @Test
  def rowtime1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.executeSql(
      """
        |CREATE TABLE user_actions (
        |  user_name STRING,
        |  data STRING,
        |  rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')), // 直接在已经存在的字段上构建 rowtime
        |  watermark for rowtime as rowtime - interval '5' minutes // rowtime 为事件时间，有乱序可能，需要声明水印纠正乱序
        |) WITH (
        |  ... flink 支持的各种 connector,只能在 blink解析器使用，1.12.x 默认解释器就是 blink
        |);
        |""".stripMargin)

    tabEnv.sqlQuery(
      """
        |select TUMBLE_START(rowtime,INTERVAL '10' MINUTES) as window_start
        |,count(distinct user_name) as uv
        |from user_actions
        |group by TUMBLE(rowtime,INTERVAL '10' MINUTES)
        |""".stripMargin)
  }

  @Test
  def rowtime2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // flink 1.12.x 开始默认使用 EventTime 作为时间语义，想要 flink 自己注入时间，需要使用 proctime
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    val seq = Seq(("Mary", "2021-05-01 12:00:00", "/home"),
      ("Bob", "2021-05-01 12:00:00", "/cart"),
      ("Mary", "2021-05-01 12:02:00", "/prod?id=1"),
      ("Mary", "2021-05-01 12:55:02", "/prod?id=4"),

      ("Bob", "2021-05-01 13:01:00", "/prod?id=5"),
      ("Liz", "2021-05-01 13:30:00", "/home"),
      ("Liz", "2021-05-01 13:59:00", "/prod?id=1"),

      ("Mary", "2021-05-01 14:00:00", "/cart"),
      ("Liz", "2021-05-01 14:02:00", "/home"),
      ("Bob", "2021-05-01 14:30:00", "/prod?id=3"),
      ("Bob", "2021-05-01 14:40:00", "/home"),
    )

    val ds = env.addSource(new StreamSourceMock[(String,String,String)](seq,false))
      .map{ t=>
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        (t._1,sdf.parse(t._2).getTime,t._3)
      }.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Long,String)]() {
        override def extractTimestamp(t: (String, Long, String), l: Long): Long = t._2
      }))
    val tabEnv = StreamTableEnvironment.create(env)
    val table = tabEnv.fromDataStream(ds, $"name", $"url", $"ctime".rowtime()) // 使用额外列，承接 flink 生成的 rowtime

    tabEnv.createTemporaryView("clicks",table)

    //    tabEnv.sqlQuery("select * from clicks").execute().print()

    // 最后一条记录，输入时窗口没有关闭，因此不会触发计算
    tabEnv.sqlQuery("""
                      |select
                      |tumble_start(ctime,interval '2' seconds) as window_start
                      |,tumble_end(ctime,interval '2' seconds) as window_end
                      |,count(distinct name) uv
                      |from clicks
                      |group by tumble(ctime,interval '2' seconds)
                      |""".stripMargin
    ).execute()
      .print()
  }

  /**
   * 时态表 TemporalTable：随时间改变，包含一个或多个版本表快照，如果存在多个时间版本就是版本表，如果只能查最新版本，就是普通表。
   * 版本表：能查任何时间版本的记录，通常包含主键和时间字段，时间字段代表版本。主键定位被查询对象。
   * 普通表：只能查当前最新的记录。
   */
  def versionTab(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    // 声明版本表，product_id 作为主键，update_time 作为时间版本（从 ddl 元数据中解析出来，是个虚拟字段，原纪录中不包含此字段）
    tabEnv.executeSql(
      """
        |CREATE TABLE product_changelog (
        |  product_id STRING,
        |  product_name STRING,
        |  product_price DECIMAL(10, 4),
        |  update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,
        |  PRIMARY KEY(product_id) NOT ENFORCED,      -- (1) 定义主键约束
        |  WATERMARK FOR update_time AS update_time   -- (2) 通过 watermark 定义事件时间
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'products',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'value.format' = 'debezium-json'
        |);
        |""".stripMargin)

    // 查找指定时间段内快照
    tabEnv.sqlQuery(
      """
        |select * from product_changelog
        |where product_id='001'
        |and update_time >= '2021-05-11 12:00:00' and update_time < '2021-05-11 12:30:00'
        |""".stripMargin)

  }

  // flink 支持基于去重查询定义版本视图
  def versionView(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    // 定义一张 append-only 表
    tabEnv.executeSql(
      """
        |CREATE TABLE RatesHistory (
        |    currency_time TIMESTAMP(3),
        |    currency STRING,
        |    rate DECIMAL(38, 10),
        |    WATERMARK FOR currency_time AS currency_time   -- 定义事件时间
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'rates',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'properties.bootstrap.servers' = 'localhost:9092',
        |  'format' = 'json'                                -- 普通的 append-only 流
        |)
        |""".stripMargin)

    // 定义版本视图 （每次只差最新的)
    tabEnv.executeSql(
      """
        |create view versioned_rates as
        |select currency, rate, currency_time from (
        | select *,
        |   row_number() over(partition by currency order by currency_time desc) as rk
        | from RatesHistory
        |) temp where rk=1
        |""".stripMargin)
  }

  /**
   *  普通表: 只能查当前最新状态
   *  基于 flink-hbase connector 连接 hbase，并将其作为时态表中的普通表进行查询
   * -- 用 DDL 定义一张 HBase 表，然后我们可以在 SQL 中将其当作一张时态表使用
   * -- 'currency' 列是 HBase 表中的 rowKey
   *
   * 理论上可以通过实现 LookupableTableSource 接口封装任意数据源，让其充当时态表，执行持续 join
   *
   */
  def normalTable(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.executeSql(
      """
        | CREATE TABLE LatestRates (
        |     currency STRING,
        |     fam1 ROW<rate DOUBLE>
        | ) WITH (
        |    'connector' = 'hbase-1.4',
        |    'table-name' = 'rates',
        |    'zookeeper.quorum' = 'localhost:2181'
        | );
        |""".stripMargin)
  }

  /**
   * 注：只有 blink planer 支持状态表
   * 时态表的运用，窗时态表函数
   * +----+--------------------------------+----------------------+----------------------+----------------------+-------------------------+-------------------------+
   * | op |                       currency |               amount |                 rate |           yen_amount |                  o_time |                  r_time |
   * +----+--------------------------------+----------------------+----------------------+----------------------+-------------------------+-------------------------+
   * | +I |                           Euro |                    2 |                  114 |                  228 | 1970-01-01T00:00:00.002 | 1970-01-01T00:00:00.001 |
   * | +I |                           Euro |                    3 |                  116 |                  348 | 1970-01-01T00:00:00.005 | 1970-01-01T00:00:00.005 |
   * | +I |                            Yen |                   50 |                    1 |                   50 | 1970-01-01T00:00:00.004 | 1970-01-01T00:00:00.001 |
   * | +I |                      US Dollar |                    1 |                  102 |                  102 | 1970-01-01T00:00:00.003 | 1970-01-01T00:00:00.001 |
   * +----+--------------------------------+----------------------+----------------------+----------------------+-------------------------+-------------------------+
   * 4 rows in set
   *
   */
  @Test
  def temporalTableFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    // 构造订单数据
    val ordersData = new collection.mutable.MutableList[(Long, String, Timestamp)]
    ordersData.+=((2L, "Euro", new Timestamp(2L)))
    ordersData.+=((1L, "US Dollar", new Timestamp(3L)))
    ordersData.+=((50L, "Yen", new Timestamp(4L)))
    ordersData.+=((3L, "Euro", new Timestamp(5L)))

    //构造汇率数据
    val ratesHistoryData = new collection.mutable.MutableList[(String, Long, Timestamp)]
    ratesHistoryData.+=(("US Dollar", 102L, new Timestamp(1L)))
    ratesHistoryData.+=(("Euro", 114L, new Timestamp(1L)))
    ratesHistoryData.+=(("Yen", 1L, new Timestamp(1L)))
    ratesHistoryData.+=(("Euro", 116L, new Timestamp(5L)))
    ratesHistoryData.+=(("Euro", 119L, new Timestamp(7L)))

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val orders = env
      .fromCollection(ordersData)
      .assignTimestampsAndWatermarks(new WatermarkTimestampExtractor[Long,String]())
      .toTable(tabEnv,'amount,'currency,'rowtime.rowtime())

    tabEnv.createTemporaryView("Orders",orders)

    val rateHistory = env.fromCollection(ratesHistoryData)
      .assignTimestampsAndWatermarks(new WatermarkTimestampExtractor[String, Long]())
      .toTable(tabEnv, 'currency, 'rate, 'rowtime.rowtime())

    tabEnv.createTemporaryView("RateHistory",rateHistory)

    val rates = rateHistory.createTemporalTableFunction('rowtime, 'currency) // 声明时间字段和主键
    tabEnv.createTemporaryFunction("Rates",rates)

     // 订单 每一行与 时态表函数创建的时态表 每一行组成成新行 （一行 * 多行 = 多行），通过 where 条件过滤指定行
//    tabEnv.sqlQuery(
//      """
//        |SELECT
//        | o.currency
//        | ,o.amount
//        | ,r.rate
//        | ,o.amount * r.rate as yen_amount
//        | ,o.rowtime as o_time
//        | ,r.rowtime as r_time
//        |FROM Orders o,
//        | LATERAL TABLE (Rates(o.rowtime)) as r
//        |WHERE o.currency = r.currency
//        |""".stripMargin
//    ).execute()
//      .print()

  }

  /**
   * 处理时间只缓存右侧(构建测),且只保存最小状态
   * +----+--------------------------------+----------------------+----------------------+----------------------+-------------------------+-------------------------+
   * | op |                       currency |               amount |                 rate |           yen_amount |                  o_time |                  r_time |
   * +----+--------------------------------+----------------------+----------------------+----------------------+-------------------------+-------------------------+
   * | +I |                           Euro |                    3 |                  114 |                  342 | 2021-05-19T01:16:13.664 | 2021-05-19T01:16:13.664 |
   * +----+--------------------------------+----------------------+----------------------+----------------------+-------------------------+-------------------------+
   *
   */
  @Test
  def temporalTableFunc2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    // 构造订单数据
    val ordersData = new collection.mutable.MutableList[(Long, String)]
    ordersData.+=((2L, "Euro"))
    ordersData.+=((1L, "US Dollar"))
    ordersData.+=((50L, "Yen"))
    ordersData.+=((3L, "Euro"))

    //构造汇率数据
    val ratesHistoryData = new collection.mutable.MutableList[(String, Long)]
    ratesHistoryData.+=(("US Dollar", 102L))
    ratesHistoryData.+=(("Euro", 114L))
    ratesHistoryData.+=(("Yen", 1L))
    ratesHistoryData.+=(("Euro", 116L))
    ratesHistoryData.+=(("Euro", 119L))

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    val orders = env
      .fromCollection(ordersData)
      .toTable(tabEnv,'amount,'currency,'proctime.proctime()) // 使用 proctime

    tabEnv.createTemporaryView("Orders",orders)

    val rateHistory = env.fromCollection(ratesHistoryData)
      .toTable(tabEnv, 'currency, 'rate, 'proctime.proctime())

    tabEnv.createTemporaryView("RateHistory",rateHistory)

    val rates = rateHistory.createTemporalTableFunction('proctime, 'currency) // 声明时间字段和主键
    tabEnv.createTemporaryFunction("Rates",rates)

    // 订单 每一行与 时态表函数创建的时态表 每一行组成成新行 （一行 * 多行 = 多行），通过 where 条件过滤指定行
    tabEnv.sqlQuery(
      """
        |SELECT
        | o.currency
        | ,o.amount
        | ,r.rate
        | ,o.amount * r.rate as yen_amount
        | ,o.proctime as o_time
        | ,r.proctime as r_time
        |FROM Orders o,
        | LATERAL TABLE (Rates(o.proctime)) as r
        |WHERE o.currency = r.currency
        |""".stripMargin
    ).execute()
      .print()

  }

  /**
   * 常规 join：左右两边表不停增长，导致要保存状态越来越多，最终导致计算不可用，因此必须有一定策略现在状态不断膨胀
   * SELECT * FROM Orders
   * INNER JOIN Product
   * ON Orders.productId = Product.id
   *
   * 时间区间 join: 只在指定时间段内进行 join，超出时间段上限，就清除状态
   * SELECT *
   * FROM Orders o ,Shipment s
   * ON o.order_id = s.order_id
   * AND o.ordertimme BETWEEN s.shiptime - INTERVAL '4' SECONDS AND s.shiptime
   *
   * 时态表 join
   * 1.只在 blink planer 下使用；
   * 2.有两种实现：
   *  a）基于时态表函数，构建笛卡尔集，然后用 where 条件匹配过滤；
   *
   * create table Order(
   *  amount bigint,
   *  currency string,
   *  rowtime timestamp(3),
   *  watermark for rowtime as rowtime - interval '10' seconds
   *  ) with (
   *   ...
   *  );
   *
   * create table RateHistory(
   *  currency string,
   *  rate bigint,
   *  rowtime timestamp(3),
   *  watermark for rowtime as rowtime - interval '10' seconds
   * ) with (
   *  ...
   * );
   *
   * // 貌似只支持 dsl 注册
   * create temporal table function rates(rowtime,currency) on RateHistory;
   *
   *
   * select
   * o.amount
   * ,o.currency
   * ,r.rate
   * ,o.amount * r.rate as yen_amount
   * ,o.rowtime as o_time
   * ,r.rowtime as r_time
   * from Order o,
   * laterval table (rates(o.rowtime)) as r
   * where o.currency=r.currency;
   *
   *
   *
   * 2）时态表 DDL join
   *
   * CREATE TABLE orders (
   *   order_id STRING,
   *   product_id STRING,
   *   order_time TIMESTAMP(3),
   *   WATERMARK FOR order_time AS order_time
   * ) WITH (
   * ...
   * );
   *
   * set table.local-time-zone = UTC;
   *
   * CREATE TABLE product_changelog (
   *   product_id STRING,
   *   product_name STRING,
   *   product_price DECIMAL(10, 4),
   *   update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL, -- 注意：自动从毫秒数转为时间戳
   *   PRIMARY KEY(product_id) NOT ENFORCED,      -- (1) defines the primary key constraint
   *   WATERMARK FOR update_time AS update_time   -- (2) defines the event time by watermark
   * ) WITH (
   *   'connector' = 'kafka',
   *   'topic' = 'products',
   *   'scan.startup.mode' = 'earliest-offset',
   *   'properties.bootstrap.servers' = 'localhost:9092',
   *   'value.format' = 'debezium-json'
   * );
   *

   *
   * select
   *  o.order_id
   *  ,o.order_time
   *  ,p.product_name
   *  ,p.product_price
   *  from orders as o
   * left join product_changelog for system_time as of o.order_time as p
   * on o.order_id = p.order_id;
   *
   * orders 为事实表
   * product_changelog 为维表
   * product_changelog for system_time as of o.order_time 以事实表 order_time 为时间基准，与维表当前能看到所有记录进行 join，
   * 由于是 left join，因此维度表不能满足事实表查询请求时，就显示 null
   *
   * 注：时态表 Join，基于事件时间时，要保存两侧状态，依赖水印清除维表(时态表)状态；
   * 如果依赖的是事件时间，只需要保存右侧(构建侧)状态，构建侧只保存最新状态，因此该状态时非常轻量级的。如果构建测可以查询外部系统，
   * 则连轻量级状态都不保存，想要直接从外部系统获取。
   *
   *
   * 1.左侧为探测表，右侧为构建表(时态表);
   * 2.左右都必须定义时间，时间为事件时间时，必须定义水印；
   * 3.右边需要定义主键，且必须是changlog流;
   * 4.使用事件时间时两侧都要保存状态，依赖水印清除状态；
   * 5.使用处理时间时，只有右侧保存状态，且只保存最新状态；
   * 6.右侧查询外部系统时，可以不保存状态，直接从外部获取
   * 7.取不到状态使用null表示。
   * +----+--------------------------------+----------------------+-------------+----------------------+-------------------------+-------------------------+
   * | op |                       currency |               amount |        rate |           yen_amount |                  o_time |                  r_time |
   * +----+--------------------------------+----------------------+-------------+----------------------+-------------------------+-------------------------+
   * | +I |                            Yen |                   50 |           1 |                   50 |     2021-05-21T12:00:04 |     2021-05-21T12:00:01 |
   * | +I |                      US Dollar |                    1 |         102 |                  102 |     2021-05-21T12:00:02 |     2021-05-21T12:00:01 |
   * | +I |                           Euro |                    8 |      (NULL) |               (NULL) |     2021-05-21T11:45:02 |                  (NULL) |
   * | +I |                           Euro |                    3 |         116 |                  348 |     2021-05-21T12:00:05 |     2021-05-21T12:00:05 |
   * | +I |                           Euro |                    2 |         114 |                  228 |     2021-05-21T12:00:02 |     2021-05-21T12:00:01 |
   * +----+--------------------------------+----------------------+-------------+----------------------+-------------------------+-------------------------+
   *
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

  @Test
  def kafkaProducerTest(): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    props.put("zookeeper.connect", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String]("test", 0, null, "null", "hello")
    producer.send(record)
    producer.flush()
    producer.close()
  }

  @Test
  def kafkaConsumerTest(): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    props.put("zookeeper.connect", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val consumer = new KafkaConsumer(props)
    consumer.listTopics().entrySet().forEach(println(_))
    consumer.close()
  }

  /**
   * +----+--------------------------------+-------------------------+-------------------------+-------------------------+
   * | op |                         symbol |             start_tmstp |            bottom_tmstp |               end_tmstp |
   * +----+--------------------------------+-------------------------+-------------------------+-------------------------+
   * | +I |                           ACME |     2021-05-11T02:00:04 |     2021-05-11T02:00:07 |     2021-05-11T02:00:08 |
   * +----+--------------------------------+-------------------------+-------------------------+-------------------------+
   */
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
   * +----+--------------------------------+-------------+-------------+-------------+
   * | op |                         symbol | START_PRICE |   TOP_PRICE |   END_PRICE |
   * +----+--------------------------------+-------------+-------------+-------------+
   * | +I |                           ACME |          12 |          25 |          14 |
   * | +I |                           ACME |          14 |          25 |          19 |
   * +----+--------------------------------+-------------+-------------+-------------+
   * 匹配成功后跳转到末尾继续往下匹配
   */
  @Test
  def matchRecognize2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val table = env.fromElements(("ACME", "2021-05-11 10:00:00", 12, 1),
      ("ACME", "2021-05-11 10:00:01", 17, 2),
      ("ACME", "2021-05-11 10:00:02", 19, 1),
      ("ACME", "2021-05-11 10:00:03", 21, 3),
      ("ACME", "2021-05-11 10:00:04", 25, 2),
      ("ACME", "2021-05-11 10:00:07", 14, 2),
      ("ACME", "2021-05-11 10:00:08", 24, 2),
      ("ACME", "2021-05-11 10:00:09", 25, 2),
      ("ACME", "2021-05-11 10:00:10", 19, 1)
    ).map { t =>
      (t._1, sdf.parse(t._2).getTime, t._3, t._4)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String, Long, Int, Int)]() {
        override def extractTimestamp(t: (String, Long, Int, Int), l: Long): Long = t._2
      })).toTable(tabEnv, 'symbol, 'ctime.rowtime(), 'price, 'tax)

    tabEnv.createTemporaryView("Ticker",table)

    /**
     * 找出连续上升，然后下降
     * LAST(A.price) 就是最后一个 A
     * LAST(A.price,1) 倒数第二个 A
     *
     * AFTER MATCH SKIP TO LAST A  匹配成功后跳转到最后一个 A (包括在内)，继续向下匹配
     * AFTER MATCH SKIP TO B 匹配成功后跳转到 B，继续向下匹配
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   FIRST(A.price) AS START_PRICE,
        |   LAST(A.price) AS TOP_PRICE,
        |   B.price AS END_PRICE
        | ONE ROW PER MATCH
        | AFTER MATCH SKIP TO B
        | PATTERN (A+ B)
        |   DEFINE
        |     A AS LAST(A.price,1) IS NULL OR A.price > LAST(A.price,1),
        |     B AS B.price < LAST(A.price,1)
        | ) MR
        |""".stripMargin
    ).execute()
      .print()

  }

  /**
   * +----+--------------------------------+-------------+-------------+-------------+
   * | op |                         symbol | START_PRICE |   TOP_PRICE |   END_PRICE |
   * +----+--------------------------------+-------------+-------------+-------------+
   * | +I |                           ACME |          12 |          25 |          14 |
   * | +I |                           ACME |          15 |          25 |          19 |
   * +----+--------------------------------+-------------+-------------+-------------+
   */
  @Test
  def matchRecognize3(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val table = env.fromElements(("ACME", "2021-05-11 10:00:00", 12, 1),
      ("ACME", "2021-05-11 10:00:01", 17, 2),
      ("ACME", "2021-05-11 10:00:02", 19, 1),
      ("ACME", "2021-05-11 10:00:03", 21, 3),
      ("ACME", "2021-05-11 10:00:04", 25, 2),
      ("ACME", "2021-05-11 10:00:07", 14, 2),  // 这是第一次匹配成功最后一行，跳过这行，从下一行开始继续向下匹配 AFTER MATCH SKIP PAST LAST ROW
      ("ACME", "2021-05-11 10:00:07", 15, 2),
      ("ACME", "2021-05-11 10:00:08", 24, 2),
      ("ACME", "2021-05-11 10:00:09", 25, 2),
      ("ACME", "2021-05-11 10:00:10", 19, 1)
    ).map { t =>
      (t._1, sdf.parse(t._2).getTime, t._3, t._4)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String, Long, Int, Int)]() {
        override def extractTimestamp(t: (String, Long, Int, Int), l: Long): Long = t._2
      })).toTable(tabEnv, 'symbol, 'ctime.rowtime(), 'price, 'tax)

    tabEnv.createTemporaryView("Ticker",table)

    /**
     * 找出连续上升，然后下降
     * LAST(A.price) 就是最后一个 A
     * LAST(A.price,1) 倒数第二个 A
     *
     * AFTER MATCH SKIP TO LAST A  匹配成功后跳转到最后一个 A (包括在内)，继续向下匹配
     * AFTER MATCH SKIP TO B 匹配成功后跳转到 B，继续向下匹配
     * AFTER MATCH SKIP PAST LAST ROW 跳过匹配成功最后一行，继续开始匹配
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   FIRST(A.price) AS START_PRICE,
        |   LAST(A.price) AS TOP_PRICE,
        |   B.price AS END_PRICE
        | ONE ROW PER MATCH
        | AFTER MATCH SKIP PAST LAST ROW
        | PATTERN (A+ B)
        |   DEFINE
        |     A AS LAST(A.price,1) IS NULL OR A.price > LAST(A.price,1),
        |     B AS B.price < LAST(A.price,1)
        | ) MR
        |""".stripMargin
    ).execute()
      .print()
  }


  /**
   * +----+--------------------------------+-------------+-------------+-------------+-------------+
   * | op |                         symbol | START_PRICE |  LAST_PRICE |   AVG_PRICE |   end_price |
   * +----+--------------------------------+-------------+-------------+-------------+-------------+
   * | +I |                           ACME |          12 |          16 |          14 |          25 |
   * | +I |                           ACME |           2 |          25 |          13 |          30 |
   * +----+--------------------------------+-------------+-------------+-------------+-------------+
   * 注：Aggregation 可以应用于表达式，但前提是它们引用单个模式变量。因此，SUM(A.price * A.tax) 是有效的，而 AVG(A.price * B.tax) 则是无效的
   * 聚合函数只能对单变量使用
   * 不支持 DISTINCT aggregation。
   */
  @Test
  def matchRecognize4(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val table = env.fromElements(
      ("ACME", "2021-05-01 10:00:00", 12,  1), // --->
      ("ACME", "2021-05-01 10:00:01", 17,  2),
      ("ACME", "2021-05-01 10:00:02", 13,  1),
      ("ACME", "2021-05-01 10:00:03", 16,  3), // <---
      ("ACME", "2021-05-01 10:00:04", 25,  2),
      ("ACME", "2021-05-01 10:00:05", 2 ,  1), // --->
      ("ACME", "2021-05-01 10:00:06", 4 ,  1),
      ("ACME", "2021-05-01 10:00:07", 10,  2),
      ("ACME", "2021-05-01 10:00:08", 15,  2),
      ("ACME", "2021-05-01 10:00:09", 25,  2), // <---
      ("ACME", "2021-05-01 10:00:10", 25,  1),
      ("ACME", "2021-05-01 10:00:11", 30,  1),
    ).map { t =>
      (t._1, sdf.parse(t._2).getTime, t._3, t._4)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String, Long, Int, Int)]() {
        override def extractTimestamp(t: (String, Long, Int, Int), l: Long): Long = t._2
      })).toTable(tabEnv, 'symbol, 'ctime.rowtime(), 'price, 'tax)

    tabEnv.createTemporaryView("Ticker",table)

    /**
     * 找出连续上升，然后下降
     * LAST(A.price) 就是最后一个 A
     * LAST(A.price,1) 倒数第二个 A
     *
     * AFTER MATCH SKIP TO LAST A  匹配成功后跳转到最后一个 A (包括在内)，继续向下匹配
     * AFTER MATCH SKIP TO B 匹配成功后跳转到 B，继续向下匹配
     * AFTER MATCH SKIP PAST LAST ROW 跳过匹配成功最后一行，继续开始匹配
     */
    // 连续几行均值小于 15，跳转到最后一行末尾，重新开始匹配
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |     FIRST(A.price) AS START_PRICE,
        |     LAST(A.price) AS LAST_PRICE,
        |     AVG(A.price) AS AVG_PRICE,
        |     B.price as end_price
        |   ONE ROW PER MATCH
        |   AFTER MATCH SKIP PAST LAST ROW
        |   PATTERN (A+ B)
        |     DEFINE
        |       A AS AVG(A.price) < 15
        | ) MR
        |""".stripMargin
    ).execute()
      .print()
  }

  /**
   * 默认按贪婪模式匹配
   * * 0或多个, >=0
   * + 1或多个, >=1
   * ? 0或1个, [0,1]
   * {n} 只有 n 个, =n
   * {n,} n个及以上，>=n
   * {,n} n个及以下, <=n
   * {m,n} m~n 个，[m,n]
   *
   * *? 对*使用勉强词模式，即 没有不存在
   *
   * 注：变量最后一位不能使用 勉强此，例如 Pattern(A B*)
   * 但可以通过设置参照变量实现 Pattern(A B*) 语义
   * Pattern(A B* C)
   *  Define
   *    A as conditionA,
   *    B as conditionB,
   *    C as not conditionB
   *
   * 不支持 B??
   */
  @Test
  def matchRecognize5(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val table = env.fromElements(
      ("XYZ", "2018-09-17 10:00:02", 10, 1),
      ("XYZ", "2018-09-17 10:00:03", 11, 2),
      ("XYZ", "2018-09-17 10:00:04", 12, 1),
      ("XYZ", "2018-09-17 10:00:05", 13, 2),
      ("XYZ", "2018-09-17 10:00:06", 14, 1),
      ("XYZ", "2018-09-17 10:00:07", 13, 1),
      ("XYZ", "2018-09-17 10:00:08", 16, 2),
    ).map { t =>
      (t._1, sdf.parse(t._2).getTime, t._3, t._4)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String, Long, Int, Int)]() {
        override def extractTimestamp(t: (String, Long, Int, Int), l: Long): Long = t._2
      })).toTable(tabEnv, 'symbol, 'ctime.rowtime(), 'price, 'tax)

    tabEnv.createTemporaryView("Ticker",table)

    /**
     * 找出连续上升，然后下降
     * LAST(A.price) 就是最后一个 A
     * LAST(A.price,1) 倒数第二个 A
     *
     * AFTER MATCH SKIP TO LAST A  匹配成功后跳转到最后一个 A (包括在内)，继续向下匹配
     * AFTER MATCH SKIP TO B 匹配成功后跳转到 B，继续向下匹配
     * AFTER MATCH SKIP PAST LAST ROW 跳过匹配成功最后一行，继续开始匹配
     */

    /**
     * 连续几行均值小于 15，跳转到最后一行末尾，重新开始匹配
     * +----+--------------------------------+-------------+-------------+-------------+-------------+
     * | op |                         symbol | START_PRICE |     B_START |      B_LAST |   END_PRICE |
     * +----+--------------------------------+-------------+-------------+-------------+-------------+
     * | +I |                            XYZ |          13 |          14 |          13 |          16 |
     * +----+--------------------------------+-------------+-------------+-------------+-------------+
     * B* 贪婪模式，即 B 能连续匹配多长就取多长
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   A.price AS START_PRICE,
        |   FIRST(B.price) AS B_START,
        |   LAST(B.price) AS B_LAST,
        |   C.price AS END_PRICE
        |  ONE ROW PER MATCH
        |  AFTER MATCH SKIP PAST LAST ROW
        |  PATTERN(A B* C)
        |  DEFINE
        |   A AS A.price > 12,
        |   B AS B.price < 15,
        |   C AS C.price > 10
        |) MR
        |""".stripMargin
    ).execute()
      .print()

    /**
     *  +----+--------------------------------+-------------+-------------+-------------+-------------+
     *  | op |                         symbol | START_PRICE |     B_START |      B_LAST |   END_PRICE |
     *  +----+--------------------------------+-------------+-------------+-------------+-------------+
     *  | +I |                            XYZ |          13 |      (NULL) |      (NULL) |          14 |
     *  | +I |                            XYZ |          13 |      (NULL) |      (NULL) |          16 |
     *  +----+--------------------------------+-------------+-------------+-------------+-------------+
     *  B*? 勉强词模式，按*含义最小解释，即出现 0 次
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   A.price AS START_PRICE,
        |   FIRST(B.price) AS B_START,
        |   LAST(B.price) AS B_LAST,
        |   C.price AS END_PRICE
        |  ONE ROW PER MATCH
        |  AFTER MATCH SKIP PAST LAST ROW
        |  PATTERN(A B*? C)
        |  DEFINE
        |   A AS A.price > 12,
        |   B AS B.price < 15,
        |   C AS C.price > 10
        |) MR
        |""".stripMargin
    ).execute()
      .print()

    /**
     * +----+--------------------------------+-------------+-------------+-------------+-------------+
     * | op |                         symbol | START_PRICE |     B_START |      B_LAST |   END_PRICE |
     * +----+--------------------------------+-------------+-------------+-------------+-------------+
     * | +I |                            XYZ |          13 |          14 |          13 |          16 |
     * +----+--------------------------------+-------------+-------------+-------------+-------------+
     * B{1,}? 按 B{1,} 最短匹配，即只匹配一次
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   A.price AS START_PRICE,
        |   FIRST(B.price) AS B_START,
        |   LAST(B.price) AS B_LAST,
        |   C.price AS END_PRICE
        |  ONE ROW PER MATCH
        |  AFTER MATCH SKIP PAST LAST ROW
        |  PATTERN(A B{1,}? C)
        |  DEFINE
        |   A AS A.price > 12,
        |   B AS B.price < 15,
        |   C AS C.price > 10
        |) MR
        |""".stripMargin
    ).execute()
      .print()
  }

  /**
   * CEP 中通过设置超时，可以阶段性清除状态，便于回收资源
   * PATTERN(A B* C) WITHIN INTERVAL '1' HOUR
   */
  @Test
  def matchRecognize6(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val table = env.fromElements(
      ("ACME", "2021-05-11 10:00:00", 20, 1),
      ("ACME", "2021-05-11 10:20:00", 17, 2),
      ("ACME", "2021-05-11 10:40:00", 18, 1),
      ("ACME", "2021-05-11 11:00:00", 11, 3),
      ("ACME", "2021-05-11 11:20:00", 14, 2),
      ("ACME", "2021-05-11 11:40:00", 9 , 1),
      ("ACME", "2021-05-11 12:00:00", 15, 1),
      ("ACME", "2021-05-11 12:20:00", 14, 2),
      ("ACME", "2021-05-11 12:40:00", 24, 2),
      ("ACME", "2021-05-11 13:00:00", 1 , 2),
      ("ACME", "2021-05-11 13:20:00", 19, 1),
    ).map { t =>
      (t._1, sdf.parse(t._2).getTime, t._3, t._4)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String, Long, Int, Int)]() {
        override def extractTimestamp(t: (String, Long, Int, Int), l: Long): Long = t._2
      })).toTable(tabEnv, 'symbol, 'ctime.rowtime(), 'price, 'tax)

    tabEnv.createTemporaryView("Ticker",table)

    /**
     * 无超时限制，A->C 下降超过 10
     * +----+--------------------------------+-------------+-------------------------+-------------+-------------+-------------+-------------------------+
     * | op |                         symbol | START_PRICE |              START_TIME |     B_START |      B_LAST |   END_PRICE |                END_TIME |
     * +----+--------------------------------+-------------+-------------------------+-------------+-------------+-------------+-------------------------+
     * | +I |                           ACME |          20 |        2021-05-11T02:00 |          17 |          14 |           9 |        2021-05-11T03:40 |
     * | +I |                           ACME |          15 |        2021-05-11T04:00 |          14 |          24 |           1 |        2021-05-11T05:00 |
     * +----+--------------------------------+-------------+-------------------------+-------------+-------------+-------------+-------------------------+
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   A.price AS START_PRICE,
        |   A.ctime AS START_TIME,
        |   FIRST(B.price) AS B_START,
        |   LAST(B.price) AS B_LAST,
        |   C.price AS END_PRICE,
        |   C.ctime AS END_TIME
        |  ONE ROW PER MATCH
        |  AFTER MATCH SKIP PAST LAST ROW
        |  PATTERN(A B* C)
        |  DEFINE
        |   B AS B.price > A.price - 10,
        |   C AS C.price < A.price - 10
        |) MR
        |""".stripMargin
    ).execute()
      .print()

    /**
     * 有超时限制，A -> C 在一小时内，下降超过 10
     * +----+--------------------------------+-------------+-------------------------+-------------+-------------+-------------+-------------------------+
     * | op |                         symbol | START_PRICE |              START_TIME |     B_START |      B_LAST |   END_PRICE |                END_TIME |
     * +----+--------------------------------+-------------+-------------------------+-------------+-------------+-------------+-------------------------+
     * | +I |                           ACME |          14 |        2021-05-11T04:20 |          24 |          24 |           1 |        2021-05-11T05:00 |
     * +----+--------------------------------+-------------+-------------------------+-------------+-------------+-------------+-------------------------+
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   A.price AS START_PRICE,
        |   A.ctime AS START_TIME,
        |   FIRST(B.price) AS B_START,
        |   LAST(B.price) AS B_LAST,
        |   C.price AS END_PRICE,
        |   C.ctime AS END_TIME
        |  ONE ROW PER MATCH
        |  AFTER MATCH SKIP PAST LAST ROW
        |  PATTERN(A B* C) WITHIN INTERVAL '1' HOUR
        |  DEFINE
        |   B AS B.price > A.price - 10,
        |   C AS C.price < A.price - 10
        |) MR
        |""".stripMargin
    ).execute()
      .print()
  }

  // 变量引用模式
  /**
   * +----+--------------------------------+-------------+-------------+-------------+
   * | op |                         symbol |     A_PRICE |     B_START |      B_LAST |
   * +----+--------------------------------+-------------+-------------+-------------+
   * | +I |                           ACME |          10 |          15 |          31 |
   * +----+--------------------------------+-------------+-------------+-------------+
   *
   */
  @Test
  def matchRecognize7(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val table = env.fromElements(
      ("ACME", "2021-05-11 10:00:00", 10, 1),
      ("ACME", "2021-05-11 10:20:00", 15, 2),
      ("ACME", "2021-05-11 10:40:00", 20, 1),
      ("ACME", "2021-05-11 11:00:00", 31, 3),
      ("ACME", "2021-05-11 11:20:00", 35, 2),
    ).map { t =>
      (t._1, sdf.parse(t._2).getTime, t._3, t._4)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String, Long, Int, Int)]() {
        override def extractTimestamp(t: (String, Long, Int, Int), l: Long): Long = t._2
      })).toTable(tabEnv, 'symbol, 'ctime.rowtime(), 'price, 'tax)

    tabEnv.createTemporaryView("Ticker", table)

    /**
     * A.price A 的price
     * SUM(price) 所有目前收录行的price
     * SUM(B.price) 所有B的price
     * 注：PATTERN(A B+ C) ，C哪怕不定义也必须有，不允许不确定个数的B结尾
     *
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE (
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   A.price AS A_PRICE,
        |   FIRST(B.price) AS B_START,
        |   LAST(B.price) AS B_LAST
        |   ONE ROW PER MATCH
        |   AFTER MATCH SKIP PAST LAST ROW
        |   PATTERN(A B+ C)
        |   DEFINE
        |     A AS A.price >= 10,
        |     B AS B.price > A.price AND SUM(price) < 100 AND SUM(B.price) < 80
        |) MR
        |""".stripMargin
    ).execute()
      .print()
  }

  // OFFSET
  @Test
  def matchRecognize8(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val table = env.fromElements(
      ("ACME", "2021-05-11 10:00:00", 10, 1),
      ("ACME", "2021-05-11 10:20:00", 15, 2),
      ("ACME", "2021-05-11 10:40:00", 20, 1),
      ("ACME", "2021-05-11 11:00:00", 31, 3),
      ("ACME", "2021-05-11 11:20:00", 35, 2),
    ).map { t =>
      (t._1, sdf.parse(t._2).getTime, t._3, t._4)
    }.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[(String, Long, Int, Int)]() {
        override def extractTimestamp(t: (String, Long, Int, Int), l: Long): Long = t._2
      })).toTable(tabEnv, 'symbol, 'ctime.rowtime(), 'price, 'tax)

    tabEnv.createTemporaryView("Ticker", table)

    /**
     *  PATTERN (A B* C)
        DEFINE
         B AS (LAST(B.price,1) IS NULL OR B.price > LAST(B.price,1)) AND
         (LAST(B.price,2) IS NULL OR B.price > 2 * LAST(B.price,2))

          B.price 大于它前面的一个 且大于它前面第二个，此处将B看成集合

        +----+--------------------------------+-------------+-------------+-------------+
        | op |                         symbol |     A_PRICE |     B_START |      B_LAST |
        +----+--------------------------------+-------------+-------------+-------------+
        | +I |                           ACME |          10 |          15 |          31 |
        +----+--------------------------------+-------------+-------------+-------------+

        Last(变量,offset) 默认从最后一个为0，然后往前排序
        First(变量,offset) 默认第一个为0，然后往后排序
        Last() First()中一个变量可以出现多次，但不能出现不同变量
     *
     */
    tabEnv.sqlQuery(
      """
        |SELECT * FROM Ticker
        |MATCH_RECOGNIZE(
        | PARTITION BY symbol
        | ORDER BY ctime
        | MEASURES
        |   A.price AS A_PRICE,
        |   FIRST(B.price) AS B_START,
        |   LAST(B.price) AS B_LAST
        | ONE ROW PER MATCH
        | AFTER MATCH SKIP PAST LAST ROW
        | PATTERN (A B* C)
        | DEFINE
        |   B AS (LAST(B.price,1) IS NULL OR B.price > LAST(B.price,1)) AND
        |   (LAST(B.price,2) IS NULL OR B.price > 2 * LAST(B.price,2))
        |) MR
        |""".stripMargin
    ).execute()
      .print()
  }


}

case class User2(name: String, score: Int, event_time: Instant)

case class OrderData(amount:Long,currency:String,ctime:String)
case class RateData(currency:String,rate:Long,ctime:Timestamp)


// 前两个字段写成泛型写活
class WatermarkTimestampExtractor[T1,T2] extends BoundedOutOfOrdernessTimestampExtractor[(T1,T2,Timestamp)](Time.seconds(10)) {
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  override def extractTimestamp(t: (T1, T2, Timestamp)): Long = t._3.getTime
}

class WatermarkTimestampExtractor2[T1] extends BoundedOutOfOrdernessTimestampExtractor[(T1,Timestamp)](Time.seconds(10)) {
  override def extractTimestamp(t: (T1, Timestamp)): Long = t._2.getTime
}