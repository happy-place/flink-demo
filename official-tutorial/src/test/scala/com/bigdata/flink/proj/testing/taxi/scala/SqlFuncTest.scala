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

class SqlFuncTest {

  @Test
  def compareFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.sqlQuery(
      """
        |select '1 = 1' as a,1 = 1 as b union all
        |select '1 <> 1' as a,1 <> 1 as b union all -- 不允许使用 !=
        |select '1 > 1' as a,1 > 1 as b union all
        |select '1 < 1' as a,1 < 1 as b union all
        |select '1 >= 1' as a,1 >= 1 as b union all
        |select '1 is null' as a,1 is null as b union all
        |select 'null is not null' as a,null is not null as b union all
        |select '1 is distinct from 1' as a,1 is distinct from 1 as b union all
        |select '1 is not distinct from "1"' as b,1 is not distinct from '1' as b union all
        |select '3 between 1 and 4' as a,3 between 1 and 4 as b union all
        |select '4 not between 1 and 4' as a,4 not between 1 and 4 as b union all
        |select '4 between SYMMETRIC 1 and 4' as a,4 between SYMMETRIC 1 and 4 as b union all
        |select '4 between ASYMMETRIC 1 and 4' as a,4 between ASYMMETRIC 1 and 4 as b union all
        |select '"apple" like "app%"' as a,'apple' like 'app%' as b union all
        |select '"banana" not like "app%"' as a,'banana' like 'app%' as b union all
        |select '"tim" similar to "Tim"' as a,'tim' similar to 'Tim' as b union all
        |select '"jack" not similar to "Tim"' as a,'jack' not similar to 'Tim' as b union all
        |select '1 in (1,2,3)' as a,1 in (1,2,3) as b union all
        |select '4 not in (1,2,3)' as a,4 not in (1,2,3) as b
        |""".stripMargin)
      .execute()
      .print()

    // between a and b 表示 [a,b] 区间 默认就是 SYMMETRIC，且 a、b中有一个为null，结果为 UNKNOWN 大写
    // between ASYMMETRIC b and a 表示 (a,b)
    // in 等效于 exist
    tabEnv.sqlQuery(
      """
        |select a,b
        |from
        |(
        | select 1 as a,10 as b union all
        | select 2 as a,20 as b union all
        | select 3 as a,30 as b
        |) t1
        |where a
        |not in (
        | select 2 as a union all
        | select 3 as a
        |)
        |""".stripMargin)
      .execute()
      .print()

    // exists 后面子查询必须是一个 boolean
    tabEnv.sqlQuery(
      """
        |select a,b
        |from
        |(
        | select 1 as a,10 as b union all
        | select 2 as a,20 as b union all
        | select 3 as a,30 as b
        |) t1
        |where exists
        |(select 1=1)
        |""".stripMargin)
      .execute()
      .print()
  }

  @Test
  def logicFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.sqlQuery(
      """
        |select '1 = 1 and 1 < 0' as a,1 = 1 and 1 < 0 as b union all
        |select '1 = 1 or 1 < 0' as a,1 = 1 or 1 < 0 as b union all
        |select 'not 1 = 1' as a,not 1 = 1 as b union all
        |select '1 = 1 is true' as a,1 = 1 is true as b union all
        |select '1 > 0 is false' as a,1 > 0 is false as b union all
        |select '1 > 0 is not false' as a,1 > 0 is not false as b union all
        |select '4 between null and 4 is UNKNOWN' as a,4 between null and 4 is UNKNOWN as b union all
        |select '4 between null and 4 is not UNKNOWN' as a,4 between null and 4 is not UNKNOWN as b
        |""".stripMargin
    ).execute()
      .print()
    //
  }

  @Test
  def arithmeticFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.sqlQuery(
      """
        |select '-1' as a,-1 as b union all
        |select '+1' as a,+1 as b union all
        |select '1 + 1' as a,+1 as b union all
        |select '1 - 1' as a,1 - 1 as b union all
        |select '1 * 1' as a,1 * 1 as b union all
        |select '1 / 2' as a,1 / 2 as b union all -- 整除
        |select '3 % 2' as a,3 % 2 as b union all -- 求余
        |select 'power(2,3)' as a,power(2,3) as b union all -- n^x
        |select 'abs(-2)' as a,abs(-2) as b union all -- 绝对值
        |select 'mod(5,2)' as a,mod(5,2) as b union all -- 取余
        |select 'sqrt(4)' as a,sqrt(4) as b union all -- 开平方
        |select 'ln(4)' as a,ln(4) as b union all -- 自然对数
        |select 'log10(100)' as a,log10(100) as b union all -- 10的对数
        |select 'log2(8)' as a,log2(8) as b union all -- 2的对数
        |select 'log(2,8)' as a,log(2,8) as b union all -- 以2为底，8的对数
        |select 'exp(1)' as a,exp(1) as b union all -- 自然对数 e 约等于 2.7183
        |select 'ceil(2.8)' as a,ceil(2.8) as b union all -- 天花板，往上够最近的整数
        |select 'ceiling(2.8)' as a,ceiling(2.8) as b union all -- 同上
        |select 'floor(2.8)' as a,floor(2.8) as b union all -- 地板，往下找最近整数
        |select 'sin(3.14/4)' as a,sin(3.14/4) as b union all -- 正弦
        |select 'sinh(0.5)' as a,sinh(0.5) as b union all
        |select 'cos(0.5)' as a,cos(0.5) as b union all
        |select 'cot(0.5)' as a,cot(0.5) as b union all
        |select 'tan(1)' as a,tan(1) as b union all
        |select 'tanh(1)' as a,tanh(1) as b union all
        |select 'asin(0.5)' as a,asin(0.5) as b union all
        |select 'acos(0.5)' as a,acos(0.5) as b union all
        |select 'atan(0.5)' as a,atan(0.5) as b union all
        |select 'cosh(0.5)' as a,cosh(0.5) as b union all
        |select 'degrees(90)' as a,degrees(90) as b union all
        |select 'radians(3.14/2)' as a,radians(3.14/2) as b union all
        |select 'sign(-1)' as a,sign(-1) as b union all -- 取符号
        |select 'round(5.6456,2)' as a,round(5.6456,2) as b union all -- 保留2位小数
        |select 'PI' as a,PI as b union all -- 最接近pi的小数
        |select 'PI()' as a,PI() as b union all -- PI
        |select 'E()' as a,E() as b union all -- 自然对数 2.7183
        |select 'rand()' as a,rand() as b union all -- 0 和 1.0 之间随机小数
        |select 'rand(6)' as a,rand(6) as b union all -- 置种随机数
        |select 'TRUNCATE(PI(),3)' as a,TRUNCATE(PI(),3) as b union all -- 截取三位小数，不进行四舍五入
        |select 'RAND_INTEGER(100)' as a,RAND_INTEGER(100) as b union all -- [0,100)内随机数
        |select 'RAND_INTEGER(50,100)' as a,RAND_INTEGER(50,100) as b -- [50,100) 内随机数
        |""".stripMargin
    ).execute()
      .print()

    // uuid
    tabEnv.sqlQuery(
      """
        |select 'UUID()' as a,UUID() as b
        |""".stripMargin
    ).execute()
      .print()

    // 十进制转二进制
    tabEnv.sqlQuery(
      """
        |select 'BIN(8)' as a,BIN(8) as b
        |""".stripMargin
    ).execute()
      .print()

    // 10进制转16进制
    tabEnv.sqlQuery(
      """
        |select 'HEX(19)' as a,HEX(19) as b
        |""".stripMargin
    ).execute()
      .print()

    // 16进制字符串
    tabEnv.sqlQuery(
      """
        |select 'HEX("hello")' as a,HEX('hello') as b
        |""".stripMargin
    ).execute()
      .print()
  }

  @Test
  def stringFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.sqlQuery(
      """select 'concat_ws("~", "AA", NULL, "BB", "", "CC")' as a ,concat_ws('~', 'AA',cast(null as varchar), 'BB', '', 'CC') as b
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select '"a" || "b"' as a ,'a' || 'b' as b union all -- 字符拼接
        |select 'upper("hello")' as a ,upper('hello') as b union all -- 大写
        |select 'lower("Hello")' as a ,lower('Hello') as b union all -- 小写
        |select 'trim(" Hel lo ")' as a ,trim(' Hel lo ') as b union all -- 删除前后空格
        |select 'ltrim(" Hel lo ")' as a ,ltrim(' Hel lo ') as b union all -- 删除左侧前后空格
        |select 'rtrim(" Hel lo ")' as a ,rtrim(' Hel lo ') as b union all -- 删除右侧前后空格
        |select 'repeat("ab",2)' as a ,repeat('ab',2) as b union all -- 指定字符串重复2次
        |select 'regexp_replace("ab12cd","//d+",'')' as a ,regexp_replace('ab12cd','//d+','') as b union all -- 指定字符串重复2次
        |select 'overlay("hello,world" placing " " from 5 for 1)' as a ,overlay('hello,world' placing ' ' from 5 for 1) as b union all -- 覆盖替换指定长度字符
        |select 'substring("abcdef" from 2 for 2)' as a ,substring('abcdef' from 2 for 2) as b union all -- 从指定位置截取指定长度子串
        |select 'replace("hello,world","hello","hi")' as a ,replace('hello,world','hello','hi') as b union all -- 从指定位置截取指定长度子串
        |select 'regexp_extract("hello,world","h(.*?)(world)",2)' as a ,regexp_extract('hello,world','h(.*?)(world)',2) as b union all -- 正则提取，指定整体
        |select 'initcap("hello")' as a ,initcap('hello') as b union all -- 正则提取，指定整体
        |select 'concat_ws("~", "AA", NULL, "BB", "", "CC")' as a ,concat_ws('~', 'AA', cast(NULL as varchar), 'BB', '', 'CC') as b union all -- 正则提取，指定整体
        |select 'lpad("hi",4,"?")' as a ,lpad('hi',4,'?') as b union all -- 提取指定长度字符，不够的使用指定字符串从左侧补齐
        |select 'rpad("hi",4,"?")' as a ,rpad('hi',4,'?') as b union all -- 提取指定长度字符，不够的使用指定字符串从右侧补齐
        |select 'FROM_BASE64("aGVsbG8gd29ybGQ=")' as a ,FROM_BASE64('aGVsbG8gd29ybGQ=') as b union all -- base64解码
        |select 'TO_BASE64("hello")' as a ,TO_BASE64('hello') as b union all -- base64编码码
        |select 'chr(97)' as a ,chr(97) as b union all -- 十进制整数转换为ascii码
        |select 'decocde(encode("hello","UTF-8"),"UTF-8")' as a ,decode(encode('hello','UTF-8'),'UTF-8') as b union all -- 二进制按指定编码进行解码，eg: 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16
        |select 'left("hello",2)' as a ,left('hello',2) as b union all -- 最左侧第二个字符
        |select 'right("hello",2)' as a ,right('hello',2) as b union all -- 最右侧第二个字符，数字小于0，返回Null
        |select 'parse_url("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1", "HOST")' as a ,parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST') as b union all -- url 解析，提取host
        |select 'parse_url("http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1", "QUERY", "k1")' as a ,parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k1') as b union all -- url解析，提取query参数中k1对应值
        |select 'split_index("a,b,c,d",",",2)' as a ,split_index('a,b,c,d',',',2) as b union all -- 按指定分隔符，分隔字符串，并返回指定下标的子串
        |select 'reverse("hello")' as a ,reverse('hello') as b union all -- 字符串反转
        |select 'substr("hello",2,3)' as a ,substr('hello',2,3) as b -- 从指定位置截取指定长度子串
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'str_to_map("a:1,b:2",",",":")' as a ,str_to_map('a:1,b:2',',',':') as b -- 按指定分隔符，将字符串拆分为map
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'encode("hello","UTF-8")' as a ,encode('hello','UTF-8') as b -- 字符串转换为指定编码的二进制，eg: 'US-ASCII', 'ISO-8859-1', 'UTF-8', 'UTF-16BE', 'UTF-16LE', 'UTF-16
        |""".stripMargin
    ).execute()
      .print()
    // [104, 101, 108, 108, 111]

    tabEnv.sqlQuery(
      """
        |select 'locate("hello","l",2)' as a ,locate('hello','l',2) as b union all -- 从指定位置(默认0)之后，指定子串出现位置，
        |select 'instr("hello","l")' as a ,instr('hello','l') as b union all -- 返回第二个字符在第一个字符串首次出现位置，任意参数为null，返回null
        |select 'ascii("abc")' as a ,ascii('abc') as b union all -- 计算给第字符串首字符的ascii，非字符串需要cast(xx as varchar) 才能计算
        |select 'char_length("hello")' as a ,char_length('hello') as b union all -- 字符长度
        |select 'character_length("hello")' as a ,character_length('hello') as b union all -- 字符长度
        |select 'position("c" in "abcdef")' as a ,position('cd' in 'abcdef') as b -- 子串出现位置，从1开始统计，只看首字母
        |""".stripMargin)
      .execute()
      .print()
  }

  @Test
  def temporalFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.sqlQuery(
      """
        |select 'DATE "2021-01-10"' as a,DATE '2021-01-10' as b -- 日期 yyyy-MM-dd
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'TIME "12:30:45"' as a,TIME '12:30:45' as b -- 时间 HH:mm:ss
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'TIMESTAMP "2021-01-10 12:30:45"' as a,TIMESTAMP '2021-01-10 12:30:45' as b -- 当前时间戳 yyyy-MM-dd HH:mm:ss[.SSS]
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'INTERVAL "10 00:00:00.004" DAY TO SECOND' as a,INTERVAL '10 00:00:00.004' DAY TO SECOND as b -- 相比UTC零点，过了10天0.004秒，经历了 864000004秒
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'INTERVAL "10" DAY' as a,INTERVAL '10' DAY as b -- 相比于UTC零点，过了10天，总共经历 864000000 秒，默认推算到秒
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'INTERVAL "2-10" YEAR TO MONTH' as a,INTERVAL '2-10' YEAR TO MONTH as b -- 相比于UTC零点，过了2年零10个月，总共间隔34个月
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'CURRENT_DATE' as a,CURRENT_DATE as b -- 日期，常量而非变量，UTC时间
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'CURRENT_TIME' as a,CURRENT_TIME as b -- 时间，UTC日期
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'CURRENT_TIMESTAMP' as a,CURRENT_TIMESTAMP as b -- 时间戳，UTC日期
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'LOCALTIME' as a,LOCALTIME as b -- 本地时间
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'LOCALTIMESTAMP' as a,LOCALTIMESTAMP as b -- 本地时间戳
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'extract(DAY FROM DATE "1990-01-28")' as a,extract(DAY FROM DATE '1990-01-28') as b -- 提取日
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'extract(MONTH FROM DATE "1990-01-28")' as a,extract(MONTH FROM DATE '1990-01-28') as b -- 提取月
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'extract(YEAR FROM DATE "1990-01-28")' as a,extract(YEAR FROM DATE '1990-01-28') as b -- 提取年
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'year(DATE "1990-01-28")' as a,year(DATE '1990-01-28') as b -- 提取年
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'month(DATE "1990-01-28")' as a,month(DATE '1990-01-28') as b -- 提取月
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'DAYOFMONTH(DATE "1990-01-28")' as a,DAYOFMONTH(DATE '1990-01-28') as b -- 提取月
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'DAYOFWEEK(DATE "1990-01-28")' as a,DAYOFWEEK(DATE '1990-01-28') as b -- 返回日期对应的周天，sunday = 1
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'DAYOFYEAR(DATE "1990-01-28")' as a,DAYOFYEAR(DATE '1990-01-28') as b -- 返回日期对应一年第几天，从1开始
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'QUARTER(DATE "1990-01-28")' as a,QUARTER(DATE '1990-01-28') as b -- 返回日期属于第几个季度
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'HOUR(TIMESTAMP "2021-02-01 11:30:45.450")' as a,HOUR(TIMESTAMP '2021-02-01 11:30:45.450') as b -- 提取时间戳对应的时
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'MINUTE(TIMESTAMP "2021-02-01 11:30:45.450")' as a,MINUTE(TIMESTAMP '2021-02-01 11:30:45.450') as b -- 提取时间戳对应的分钟
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'SECOND(TIMESTAMP "2021-02-01 11:30:45.450")' as a,SECOND(TIMESTAMP '2021-02-01 11:30:45.450') as b -- 提取时间戳对应的秒
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'FLOOR(TIME "12:44:31" TO MINUTE)' as a,FLOOR(TIME '12:44:31' TO MINUTE) as b -- 地板，抹平到分钟
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'CEIL(TIME "12:44:31" TO MINUTE)' as a,CEIL(TIME '12:44:31' TO MINUTE) as b -- 天花板，往上够到最近分钟
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select '(TIME "02:44:31", INTERVAL "1" HOUR) OVERLAPS (TIME "03:21:31", INTERVAL "2" HOUR)' as a,(TIME '02:44:31', INTERVAL '1' HOUR) OVERLAPS (TIME '03:21:31', INTERVAL '2' HOUR) as b union all -- 时间段是否存在搭接 TRUE
        |select '(TIME "02:44:31", TIME "03:44:31") OVERLAPS (TIME "03:45:31", TIME "05:45:31")' as a,(TIME '02:44:31', TIME '03:44:31') OVERLAPS (TIME '03:45:31', TIME '05:45:31') as b --  FALSE
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'DATE_FORMAT(TIMESTAMP "2001-01-02 11:30:34","yyyy/MM/dd HH:mm:ss")' as a,DATE_FORMAT(TIMESTAMP '2001-01-02 11:30:34','yyyy/MM/dd HH:mm:ss') as b -- 日期格式化
        |""".stripMargin
    ).execute()
      .print()


    tabEnv.sqlQuery(
      """
        |select 'TIMESTAMPADD(DAY,2,DATE "1990-02-23")' as a,TIMESTAMPADD(DAY, 2, DATE '1990-02-23') as b -- 指定日期上发生指定偏移量, 偏移单位：YEAR、MONTH、QUARTER、WEEK、DAY、HOUR、MINUTE、SECOND
        |""".stripMargin
    ).execute()
      .print()

    tabEnv.sqlQuery(
      """
        |select 'TIMESTAMPDIFF(DAY,2,TIMESTAMP "1990-02-23 12:00:00",TIMESTAMP "1990-02-25 13:00:00")' as a,TIMESTAMPDIFF(DAY, TIMESTAMP '1990-02-23 12:00:00', TIMESTAMP '1990-02-25 13:00:00') as b -- 指定两日期间隔
        |""".stripMargin
    ).execute()
      .print()

  }

}
