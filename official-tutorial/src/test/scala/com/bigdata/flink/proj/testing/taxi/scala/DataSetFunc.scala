package com.bigdata.flink.proj.testing.taxi.scala

import org.apache.flink.api.common.functions.{FlatMapFunction, GroupCombineFunction, GroupReduceFunction, MapFunction, Partitioner, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.java.io.LocalCollectionOutputFormat
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.scala.utils.DataSetUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.junit.Test
import org.apache.flink.hadoopcompatibility.scala.HadoopInputs
import org.apache.flink.util.Collector

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.{lang, util}
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{JobConf, TextInputFormat}
import org.apache.hadoop.mapreduce.Job

import scala.collection.mutable.ListBuffer

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/10 9:29 上午 
 * @desc:
 *
 */

case class T1(var id:Int)


case class Complex(id:String,arr:Array[Int],tup:(String,Int))
case class Body(id:String,complex:Complex)
case class Book(id:String,price:Double)
case class User(userIdentifier:Int,name:String)
case class Email(userId:Int,subject:String,body:String)

class DataSetFunc {

  /**
   * 1.环境是ExecutionEnvironment，且末尾无序使用 env.execute() 触发；
   * 2.groupBy 代替 keyBy 进行分组；
   * 3.一次性输出最终结果，而不是输入一个元素，累计计算一次
   *
   * flatMap： 输入一个，可以多次输出，输入输出元素可以不同，有 collector 可以进行 filter 判断，觉得元素是否输出；
   * map: 输入一个，必须输出一个，且输入与输出类型要完全一致；
   * groupBy: 分组 类似 keyBy
   * sum: 聚合函数，可以执行下标或字段名
   *
   */
  @Test
  def wordcount(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements("hello,java","hi,python","oh,java")
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0) // 注：DataStream 中是 keyBy，DataSet 中是 groupBy
      .sum(1)
      .print()
  }

  /**
   * mapPartition 与 spark 中 mapPartition 功能一致，
   * 数据处理如果需要访问外部资源时，为避免反复创建连接(one by one)，以分区为单位创建一次连接批量处理数据
   */
  @Test
  def mapPartition(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements("hello,java","hi,python","oh,java")
      .flatMap(_.split(","))
      .mapPartition{it =>
        it.map((_,1))
      }.groupBy(0)
      .sum(1)
      .print()
  }

  /**
   * filter 中只进行 bool 判断，不修改元素内容，否则可能出现异常
   */
  @Test
  def filter(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(T1(1),T1(2),T1(3))
      .filter{t =>
//        t.id = 4
        t.id % 2 != 0
      }.print()
  }

  // 没有 key 则直接累加，并且一次性输出结果
  @Test
  def reduce1(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(1,2,3)
      .reduce(_ + _)
      .print()
  }

  // 有 key,需要先 groupBy 分组，然后聚合
  @Test
  def reduce2(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements("hello,java","hi,python","oh,java")
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .reduce((i1,i2) => (i1._1,i1._2 + i2._2))
      .print()
  }

  /**
   * 分组进行累计：一次性对整个分区数据进行处理，输入类型与输出类型要一致
   */
  @Test
  def reduceGroup1(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements("hello,java","hi,python","oh,java")
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .reduceGroup{it =>
        val list = it.toList
        val key = list.head._1
        val value = list.map(_._2).sum
        (key,value)
      }.print()
  }

  // 一次性处理整个分区，没有 key 情况下直接 sum 输出
  @Test
  def reduceGroup2(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(1,2,3,4,5,6)
      .reduceGroup{it =>
        it.sum
      }.print()
  }

  // 分桶，然后在桶内比较大小
  @Test
  def agg1(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.fromElements(("hello",1),("hello",2),("hi",1),("hi",3),("python",1))
      .groupBy(0)
      .max(1)
      .print()
  }

  // (java,2)
  @Test
  def agg2(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements("hello,java","hi,python","oh,java")
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .aggregate(Aggregations.SUM,1) // 结合前面 groupBy 进行聚合 等价于 sum(1)
      .map(i => (1,i._1,i._2)) // 重加分区
      .groupBy(0) // 全局桶
      .aggregate(Aggregations.MAX,2) // 全局排序取最大值 等价于 max(2)
      .map(i => (i._2,i._3)) // 摘掉全局桶标识
      .print()

    // 上面等价于下面，使用 maxBy minBy 始终都能得到正确结果
    env.fromElements("hello,java","hi,python","oh,java")
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .maxBy(1)
      .print()

  }

  /**
   * 注： maxBy 、minBy、自始至终都是正确结果。max，min设计 kv 结构只能保证 value 是想要的极值，key 就不保证正确性了
   * 第一次结果 (java,2)、第二次结果 (oh,2)，
   * value 始终都是最大值，key 就不保证了
   */
  @Test
  def agg22(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements("hello,java","hi,python","oh,java")
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .max(1)
      .print()
  }

  // (java,2) 反复验证都是正确结果
  @Test
  def agg23(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements("hello,java","hi,python","oh,java")
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .maxBy(1)
      .print()
  }

  // 直接去重
  @Test
  def distinct1(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(1,2,3,4,2,1)
      .distinct()
      .print()
  }

  // 处理 tuple 按指定元素下标去重，留下第一个
  @Test
  def distinct2(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(("hello",1),("hello",2),("hi",1),("hi",3),("python",1))
      .distinct(0) // 同理如果是 case class 可以设置按指定字段名去重
      .print()
  }

  /**
   * (hello,2,101)
   * (hello,1,100)
   * 对 下标 0、2元素进行去除，重复元素保留第一个
   */
  @Test
  def distinct3(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(("hello",1,100),("hello",2,100),("hello",2,101))
      .distinct(0,2) // 同理如果是 case class 可以设置按指定字段名去重
      .print()
  }

  /**
   * 1
   * 0
   * 2
   * 函数去重
   */
  @Test
  def distinct4(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(1,-1,0,2)
      .distinct(Math.abs(_))  // 函数生成结果是唯一的，如果有重复保留第一次出现的
      .print()
  }

  @Test
  def distinct5(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(Book("1",11.1),Book("2",11.1),Book("2",21.1))
      .distinct("price") // case class 字段名进行去除
      .print()
  }

  @Test
  def distinct6(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(Book("1",11.1),Book("1",11.1),Book("2",11.1),Book("2",21.1))
      .distinct("_") // 按整个对象进行去除
      .print()
  }

  // 去重留下最后一个元素
  @Test
  def reduceGroup(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(("hello",1),("hello",2),("hi",1),("hi",3),("python",1))
      .groupBy(0)
      .reduceGroup{it =>
        val list = it.toList
        val key = list.head._1
        val value = list(list.size-1)._2
        (key,value)
      }.print()
  }

  /**
   * flink join 两种机制
   *
   * JoinHint.REPARTITION_HASH_FIRST 两个表都进行 shuffle 重分区(除非已经分好区)，key 相同数据归到一个分区，对小表（第一个)数据分片创建哈希表，大表数据分片参照此哈希表进行连接。（存在大小表关系，且两张表数据都很大）
   * JoinHint.REPARTITION_HASH_SECOND 两个表都进行 shuffle 重分区(除非已经分好区)，key 相同数据归到一个分区，对小表（第二个)数据分片创建哈希表，大表数据分片参照此哈希表进行连接。存在大小表关系，且两张表数据都很大）
   * JoinHint.BROADCAST_HASH_FIRST 对小表(第一张)表进行广播(小表全量分发到大表数据所在 slot)，并对其创建哈希表，大表(第二张)对其进行输入探测，实现 Join；（存在大小表关系，某张表数据非常小)
   * JoinHint.BROADCAST_HASH_SECOND 对小表(第二张)表进行广播(小表全量分发到大表数据所在 slot)，并对其创建哈希表，大表(第二张)对其进行输入探测，实现 Join；（存在大小表关系，某张表数据非常小)
   *
   * JoinHint.OPTIMIZER_CHOOSES  交给优化器觉得，join 默认策略
   * JoinHint.REPARTITION_SORT_MERGE 同时对两张表进行 shuffle 重分区(除非它已经实现分好区)，key 相同数据归到一个分区，分布对两种表数据进行排序(除非已经排好序)，基于排序进行流合并实现数据连接。（有其中有表已经重分区并排好序时可以使用)
   *
   * 1.Join 策略默认交给选择器处理；
   * 2.存在大小表关系时，考虑对小表创建哈希表，大表数据量可以接受 shuffle 时，选择对两张表都进行重分区，并对小表创建哈希表
   * 2.存在大小表关系时，考虑对小表创建哈希表，大表数据量太大，选择对小表广播，并创建哈希表；
   * 3.两张表数据量都嫩大，且都存在一定重复率，选择重分区排序合并。
   *
   * inner join
   * ((a,1),(a,100))
   * ((a,2),(a,100))
   * ((c,3),(c,300))
   * ((c,3),(c,310))
   *
   *
   * join可以使用
   *  REPARTITION_HASH_FIRST
   *  REPARTITION_HASH_SECOND
   *  BROADCAST_HASH_FIRST
   *  BROADCAST_HASH_SECOND
   *  OPTIMIZER_CHOOSES
   *  REPARTITION_SORT_MERGE
   *
   * leftOuterJoin 可以使用
   *  REPARTITION_HASH_SECOND
   *  BROADCAST_HASH_SECOND
   *  OPTIMIZER_CHOOSES
   *  REPARTITION_SORT_MERGE
   *
   * rightOuterJoin
   *  REPARTITION_HASH_FIRST
   *  BROADCAST_HASH_FIRST
   *  OPTIMIZER_CHOOSES
   *  REPARTITION_SORT_MERGE
   *
   * fullOuterJoin
   *  OPTIMIZER_CHOOSES
   *  REPARTITION_SORT_MERGE
   */
  @Test
  def join1(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(("a",1),("a",2),("b",2),("c",3))
    val ds2 = env.fromElements(("a",100),("c",300),("c",310),("d",400))
    ds1.join(ds2) // 默认 JoinHint.OPTIMIZER_CHOOSES 优化器自己定
      .where(_._1).equalTo(_._1)
      .print()
  }

  // 第一张报是小表，大表数据量非常大，现在广播第一张表，并对创建哈希表
  @Test
  def join2(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(("a",1),("a",2),("b",2),("c",3))
    val ds2 = env.fromElements(("a",100),("c",300),("c",310),("d",400))
    ds1.join(ds2,JoinHint.BROADCAST_HASH_FIRST) // JoinHint.BROADCAST_HASH_SECOND
      .where(_._1).equalTo(_._1)
      .print()
  }

  // 第一张表是小表，大表数据量可以接受 shuffle，两张表同时窗口重分区，并对第一张表创建哈希表
  @Test
  def join3(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(("a",1),("a",2),("b",2),("c",3))
    val ds2 = env.fromElements(("a",100),("c",300),("c",310),("d",400))
    ds1.join(ds2,JoinHint.REPARTITION_HASH_FIRST) // JoinHint.REPARTITION_HASH_SECOND
      .where(_._1).equalTo(_._1)
      .print()
  }

  // 两张表都很大，且重复率比较高，则对两种表都进行重分区，排序合并
  @Test
  def join4(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(("a",1),("a",2),("b",2),("c",3))
    val ds2 = env.fromElements(("a",100),("c",300),("c",310),("d",400))
    ds1.join(ds2,JoinHint.REPARTITION_SORT_MERGE) //
      .where(_._1).equalTo(_._1)
      .print()
  }

  @Test
  def join5(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(Book("1",11.1),Book("2",12.1),Book("3",13.1))
    val ds2 = env.fromElements(("1",100),("3",300),("3",310),("4",400))
    ds1.join(ds2,JoinHint.REPARTITION_SORT_MERGE) //
      .where("id").equalTo(_._1)
      .print()
  }

  /**
   * ((a,1),(a,100))
   * ((a,2),(a,100))
   * ((b,2),None)
   * ((c,3),(c,300))
   * ((c,3),(c,310))
   */
  @Test
  def leftOuterJoin(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(("a",1),("a",2),("b",2),("c",3))
    val ds2 = env.fromElements(("a",100),("c",300),("c",310),("d",400))
    ds1.leftOuterJoin(ds2,JoinHint.REPARTITION_SORT_MERGE) // 同理使用 outerJoin
      .where(_._1).equalTo(_._1).apply{
        (l,r) =>
        if(r == null){
          (l,None)
        }else{
          (l,r)
        }
    }.print()
    ds1.fullOuterJoin(ds2,JoinHint.REPARTITION_SORT_MERGE) // 同理使用 outerJoin
      .where(_._1).equalTo(_._1).apply{
      (l,r) =>
        if(r == null){
          (l,None)
        }else if(l == null){
          (None,r)
        }else{
          (l,r)
        }
    }.print()
  }

  /**
   * 全外连接
   * ((a,1),(a,100))
   * ((a,2),(a,100))
   * ((b,2),None)
   * (None,(d,400))
   * ((c,3),(c,300))
   * ((c,3),(c,310))
   */
  @Test
  def fullOuterJoin(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(("a",1),("a",2),("b",2),("c",3))
    val ds2 = env.fromElements(("a",100),("c",300),("c",310),("d",400))
    ds1.fullOuterJoin(ds2,JoinHint.REPARTITION_SORT_MERGE) // 同理使用 outerJoin
      .where(_._1).equalTo(_._1).apply{
      (l,r) =>
        if(r == null){
          (l,None)
        }else if(l == null){
          (None,r)
        }else{
          (l,r)
        }
    }.print()
  }


  /**
   * 左右先分组形成集合然后连接，全外连接
   * (a,1),(a,2) -> (a,100)
   * (b,2) ->
   *  -> (d,400)
   * (c,3) -> (c,300),(c,310)
   */
  @Test
  def coGroup(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(("a",1),("a",2),("b",2),("c",3))
    val ds2 = env.fromElements(("a",100),("c",300),("c",310),("d",400))
    ds1.coGroup(ds2) //
      .where(_._1).equalTo(_._1).apply{
      (l,r) =>
       s"${l.mkString(",")} -> ${r.mkString(",")}"
    }.print()
  }

  /**
   * 形成笛卡尔积，即便对于大集群也是非常大调整，推荐使用 crossWithHuge 或 crossWithTiny
   * Cross is potentially a very compute-intensive operation which can challenge even large compute clusters!
   * (a,1)
   * (a,2)
   * (b,1)
   * (b,2)
   * (c,1)
   * (c,2)
   */
  @Test
  def cross(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements("a","b","c")
    val ds2 = env.fromElements(1,2)
    ds1.cross(ds2)
      .print()
  }

  // 求点两两间距离
  @Test
  def cross2(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements((1,2),(2,3),(3,4))
    ds1.cross(ds1){(p1,p2) =>
      val dist = Math.sqrt(Math.pow(p1._1 - p2._1,2) + Math.pow(p1._2 - p2._2,2))
      (p1,p2,dist)
    }.print()
  }

  // 后者大
  @Test
  def crossWithHuge(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements("a","b","c")
    val ds2 = env.fromElements(1,2)
    ds1.crossWithHuge(ds2)
      .print()
  }

  // 后者小
  @Test
  def crossWithTiny(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements("a","b","c")
    val ds2 = env.fromElements(1,2)
    ds1.crossWithTiny(ds2)
      .print()
  }

  // 相同数据结构 DS 拼接到一起
  @Test
  def union(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(("a",1),("a",2),("b",2),("c",3))
    val ds2 = env.fromElements(("a",100),("c",300),("c",310),("d",400))
    ds1.union(ds2)
      .print()
  }

  // 轮询重分布
  @Test
  def rebalance(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(("a",1),("a",2),("b",2),("c",3))
    ds1.rebalance()
      .mapPartition{ it =>
        val list = it.toList
        println(s"${list.size} ${list}")
        list
      }.print()
  }

  /**
   * 对 key 取 hash 划定分区
   * 1.相同 key 分到同一个分区
   * 2.相邻 key 不再同一个分区
   */
  @Test
  def partitionByHash(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val ds1 = env.fromElements(("a",1),("a",2),("b",2),("c",3))
    ds1.partitionByHash(0)
      .mapPartition{ it =>
        val list = it.toList
        println(s"${list.size} ${list}")
        list
      }.print()
  }

  /**
   * 依据 key 范围划定分区
   * 1.相同 key 分到同一个分区
   * 2.相邻 key 可能分到同一个分区
   */
  @Test
  def partitionByRange(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val ds1 = env.fromElements(("a",1),("a",2),("b",2),("c",3),("d",4),("e",5))
    ds1.partitionByRange(0)
      .mapPartition{ it =>
        val list = it.toList
        println(s"${list.size} ${list}")
        list
      }.print()
  }

  /**
   * 自定义分区
   */
  @Test
  def partitionCustom(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val ds1 = env.fromElements(("a",1),("a",2),("b",2),("c",3),("d",4),("e",5))
    ds1.partitionCustom(new Partitioner[String] {
        override def partition(key: String, i: Int): Int = key.hashCode % i
      },0)
      .print()
  }

  /**
   * (a,3),(a,2),(a,1)
   * sortPartition 分区中安指定元素 指定顺序进行排序
   */
  @Test
  def sortPartition(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(("a",3),("a",1),("a",2),("b",2),("c",3),("d",4),("e",5))
    ds1.partitionCustom(new Partitioner[String] {
      override def partition(key: String, i: Int): Int = key.hashCode % i
    },0).sortPartition(1,Order.DESCENDING)
      .mapPartition{in =>
        val list = in.toList
        println(list.mkString(","))
        in.toList
      }
      .print()
  }

  /**
   * first 配合 group 使用，取每个分组最先到达 n 个元素 （没有排序)，因此通常同需要彭 sortGroup 使用 取 topN
   * (b,0),(b,4)
   * (a,3),(a,2)
   *
   * 注：sortPartition 对分区进行排序，同分区你数据的 key 不一定是相同的；
   * sortGroup 对分组进行排序，组里面数据的 key 一定是相同的
   *
   */
  @Test
  def first(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(("a",3),("a",2),("a",1),("a",4),("b",0),("b",4),("b",2),("b",1))
      .groupBy(0)
      .first(2) // 取收到的头两个元素，没有经过排序
      .mapPartition{it =>
        val list = it.toList
        if(list.size>0){
          println(list.mkString(","))
        }
        list
      }.print()
  }

  /**
   * (b,4),(b,2)
   * (a,4),(a,3)
   */
  @Test
  def sortGroup(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(("a",3),("a",2),("a",1),("a",4),("b",0),("b",4),("b",2),("b",1))
      .groupBy(0)
      .sortGroup(1,Order.DESCENDING)
      .first(2) // 取前面排序后的 topN
      .mapPartition{it =>
        val list = it.toList
        if(list.size>0){
          println(list.mkString(","))
        }
        list
      }.print()
  }

  /**
   * 有 groupBy 分组，组内求极值，极值存在多个，只输出第一个
   * 无 groupBy 分组，全局求极值，极值存在多个，只输出第一个
   * 分组内求最大值，组内如果存在多个最大值，只输出第一个
   */
  @Test
  def maxBy(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(("a",4,1),("a",2,2),("a",1,3),("a",4,4),("b",0,5),("b",4,6),("b",2,7),("b",1,8))
      .groupBy(0)
      .maxBy(1)
      .print()
  }

  // 全局求 max
  @Test
  def maxBy2(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(("a",4,1),("a",2,2),("a",1,3),("a",4,4),("b",0,5),("b",4,6),("b",2,7),("b",1,8))
      .maxBy(1)
      .print()
  }

  // 多级分组求 max 前求下标为 1 位置，找到多个时，在按下标为 2 继续找极值
  @Test
  def maxBy3(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(("a",4,1),("a",2,2),("a",1,3),("a",4,4),("b",0,5),("b",4,6),("b",2,7),("b",1,8))
      .groupBy(0)
      .maxBy(1,2)
      .print()
  }

  // 多级分组
  @Test
  def groupBy(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(("a",4,1),("a",2,2),("a",2,3),("a",4,4),("b",0,5),("b",4,6),("b",2,7),("b",2,8))
      .groupBy(0,1)
      .reduceGroup{it =>
        val list = it.toList
        println(list.mkString(","))
        list
      }
      .print()
  }

  /**
   * minBy、maxBy、groupBy 都可以写字符串表达式
   */
  @Test
  def groupByExpress(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.fromElements(Body("1",Complex("1",Array(1,2,3),("a",1))))
//      .groupBy(_.id) // 字段名称
//      .groupBy("complex.id") // 符合类型字段
//      .groupBy("complex.tup._1") // 元祖下划线下标
      .groupBy("complex.tup.1") // 元祖位置下标
      .reduceGroup{it =>
        val list = it.toList
        println(list.mkString(","))
        list
      }
      .print()
  }

  /**
   * 输入
   * 支持读取：default、gzip、bzip2、xz 压缩格式
   */
  def read(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.readTextFile("file:///path/to/my/textfile") // 读取本地文件
    env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile") // 读取 hdfs
    env.readCsvFile[(String,Int,Double)]("hdfs:///the/CSV/file") // 读取 csv
    env.readCsvFile[(String,Double)]("hdfs:///the/CSV/file",includedFields =Array(0,3)) // 读取指定列
    /**
     * lineDelimiter 行分割符
     * fieldDelimiter 字段分割符
     * pojoFields： case class 映射字段
     * parseQuotedStrings：解析引号
     * ignoreComments： 忽略注释
     * lenient: 对布尔值进行宽松解析
     * ignoreFirstLine：忽略头行
     */

    case class Book(id:String,price:Double)
    val ds =env.readCsvFile[Book]("hdfs:///the/CSV/file",includedFields = Array(0,3)) //case class 组织数据

    case class Book2(price:Double,id:String)
    env.readCsvFile[Book2]("hdfs:///the/CSV/file2",includedFields = Array(0,3),pojoFields = Array("id","price")) // 自定字段映射关系

    env.fromElements("a","b","c") // 可变集合
    env.generateSequence(0,100) // 序列 [0,100)

    val tups = env.createInput(HadoopInputs.readSequenceFile(classOf[IntWritable],classOf[Text],"hdfs://nnHost:nnPort/path/to/file")) // 读取 hadoop seq file


    // 常规 readTextFile 只会读取自定目录下的文件不会递归扫描文件夹，设置 recursive.file.enumeration，可以递归扫描文件夹
    val conf = new Configuration()
    conf.setBoolean("recursive.file.enumeration",true)
    env.readTextFile("file:///path/with.nested/files").withParameters(conf)

  }

  def write(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    var ds:DataSet[(String,Int,Double)] = env.readCsvFile[(String,Int,Double)]("hdfs:///the/CSV/file2")
    ds.writeAsText("file:///my/result/on/localFS") // 落盘到本地 （tuple 格式)
    ds.writeAsText("hdfs://nnHost:nnPort/my/result/on/localFS",WriteMode.OVERWRITE) // 落盘到hdfs 覆盖写入
    ds.writeAsCsv("hdfs://nnHost:nnPort/my/result/on/localFS",rowDelimiter = "\n",fieldDelimiter = "|") // 指定行分割符，字段分割符

    ds.map(i => i._1+"_"+i._3).writeAsText("file:///my/result/on/localFS") // 拼接后输出

  }

  /**
   * sortPartition, 目前只支持分区排序，不支持全局排序
   */
  def sortPartition2(): Unit ={
    case class Book(id:String,price:Double)
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tData = env.readCsvFile[(Int, String, Double)]("hdfs:///the/CSV/file2")
    val pData: DataSet[(Book, Double)] = env.readCsvFile[(Book, Double)]("hdfs:///the/CSV/file2")
    val sData: DataSet[String] = env.readTextFile("hdfs:///the/CSV/file2")

    tData.sortPartition(1,Order.DESCENDING).print() // 按元祖下标为 1 元素排序
    tData.sortPartition(2,Order.DESCENDING).sortPartition(0,Order.ASCENDING).print() // 元祖下标 2 字段降序，下标 1 字段生效排序
    pData.sortPartition("_1.id",Order.DESCENDING).writeAsText("hdfs://nnHost:nnPort/my/result/on/localFS") // 元祖第1 个元素的 id 排序
    tData.sortPartition("_",Order.DESCENDING).print() // 按整个元祖排序
    sData.sortPartition("_",Order.DESCENDING).print() // 按字符串排序
  }

  /**
   * 蒙特卡罗算法计算圆周率
   * 创建建 半径为 1 的园内投 N 次石子，每次石子落地位置到圆心，落地在圆内，在之前分数基础上加 1，否则加 0，起始分数为 0 分。
   * 投 n 次后，分数变为 Sn，则 Pi 约等于 Sn / n * 4 ，随投递次数增大，Pi 误差越小
   */
  @Test
  def bulkIteration(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(0)

    val maxIterations = 10000

    val count = ds.iterate(maxIterations){it =>
      it.map{ i =>
        val x = Math.random()
        val y = Math.random()
        i + (if(x * x + y * y <1) 1 else 0)
      }
    }

    val result = count.map( _ / (maxIterations * 1.0) * 4)
    result.print()

  }

  // 广播数据集
  @Test
  def broadcast(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(1,2)
    val ds2 = env.fromElements("a","b")
    val broadcastName = "broadcast"
    ds1.map(new RichMapFunction[Int,Int] {
      override def map(in: Int): Int = {
        val strings = getRuntimeContext.getBroadcastVariable[String](broadcastName) // 从环境中提取
        println(strings)
        in
      }
    }).withBroadcastSet(ds2,broadcastName) // 广播 DataSet
      .print()
  }

  @Test
  def distributeCache(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val cacheName = "args/test.properties"
    val path = getClass.getClassLoader.getResource(cacheName).getPath
    env.registerCachedFile(path,cacheName,false) // 寄存分布式缓存
    val ds = env.fromElements(1,2,3)
    ds.map(new RichMapFunction[Int,Int] {
      override def map(in: Int): Int = {
        val file = getRuntimeContext.getDistributedCache.getFile(cacheName)
        val line = new BufferedReader(new InputStreamReader(new FileInputStream(file))).readLine // 读取分布式缓存
        println(line)
        in
      }
    }).print()
  }

  @Test
  def passArgs1(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(1,2,3)

     // 构造传参
    class MyMap(limit:Int) extends FlatMapFunction[Int,Int]{
      override def flatMap(t: Int, collector: Collector[Int]): Unit = {
        if(t > limit){
          collector.collect(t)
        }
      }
    }

    ds.flatMap(new MyMap(2)).print()
  }

  @Test
  def passArgs2(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(1,2,3)
    val conf = new Configuration()
    val confName = "limit"

    conf.setInteger(confName,2)

    // 配置传参
    class MyMap extends RichFlatMapFunction[Int,Int]{
      private var limit:Int = _

      override def open(parameters: Configuration): Unit = {
        limit = parameters.getInteger(confName,0)
      }

      override def flatMap(t: Int, collector: Collector[Int]): Unit = {
        if(t > limit){
          collector.collect(t)
        }
      }
    }

    ds.flatMap(new MyMap).withParameters(conf).print()
  }

  @Test
  def passArgs3(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(1,2,3)
    val conf = new Configuration()
    val confName = "limit"

    conf.setInteger(confName,2)

    env.getConfig.setGlobalJobParameters(conf)

    // 全局配置传参
    class MyMap extends RichFlatMapFunction[Int,Int]{
      private var limit:Int = _

      override def open(parameters: Configuration): Unit = {
        val conf = getRuntimeContext.getExecutionConfig.getGlobalJobParameters().asInstanceOf[Configuration]
        limit = conf.getInteger(confName,0)
      }

      override def flatMap(t: Int, collector: Collector[Int]): Unit = {
        if(t > limit){
          collector.collect(t)
        }
      }
    }

    ds.flatMap(new MyMap).withParameters(conf).print()
  }


  /**
   * 有大 key 存在是，直接使用 reduceGroup 分组聚合，可能导致数据倾斜，可以先使用 GroupCombineFunction 进行预聚合（slots 内部)，然后使用
   * reduceGroup 拉取分散在不同 slots 上相同 key 小聚合结果继续聚合，使 reduce 处理具有并行效果，能显著提升计算效率
   * combine 逻辑与 reduce 要一致，combine 输出对应 reduce 输入
   *
   * 注： generateSequence 产生的是Long 类型序列
   */
  @Test
  def combineBeforeGroupReduce(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment

    class PreCombineGroupReduce extends GroupReduceFunction[(String,Long),(String,Long)] with GroupCombineFunction[(String,Long),(String,Long)] {
      override def reduce(iterable: lang.Iterable[(String,Long)], collector: Collector[(String,Long)]): Unit = {
        val list = iterable.toList
        println(s"reduce-size: ${list.size}")
        val key = list.head._1
        val value = list.map(_._2).sum
        collector.collect((key,value))
      }

      override def combine(iterable: lang.Iterable[(String, Long)], collector: Collector[(String,Long)]): Unit = {
        val list = iterable.toList
        println(s"combine-size: ${list.size}")
        val key = list.head._1
        val value = list.map(_._2).sum
        println(s"combine-size: ${list.size}, value: ${value}")
        collector.collect((key,value))
      }
    }

    env.generateSequence(1,100).map(("a",_))
      .groupBy(0)
      .reduceGroup(new PreCombineGroupReduce)
      .print
  }

  /**
   * 同时对 tuple 多个下标进行分别聚合，然后用一个 tuple 带出结果，当 and 跟前面的聚合时同一个下标时，会覆盖前面的
   * (a,2,1)
   */
  @Test
  def and(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 基于下标 0 分组，对下标 1 取最大值，对下标 2 取最小值
    env.fromElements(("a",1,2),("a",2,1))
      .groupBy(0)
      .aggregate(Aggregations.MAX,1) // 等效于 max(1)
      .and(Aggregations.MIN,2) // 等效于 min(2)
      .print()
  }

  /**
   * 连续使用两个聚合时，后面一个聚合在掐你个聚合基础上执行
   */
  @Test
  def notAnd(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 基于下标 0 分组，组内对下标 1 求和，然后在求和基础上对下标 2 取最小值
    env.fromElements(("a",1,2),("a",2,1),("b",1,3),("b",2,1))
      .groupBy(0)
      .aggregate(Aggregations.SUM,1) // (a,3,1) (b,3,1)
      .aggregate(Aggregations.MIN,2) //  (a,3,1)
      .print()
  }

  /**
   * 生成连续唯一标识(索引)，由于需要保持连续，因此数据加工可能效率比较低
   */
  @Test
  def zipWithIndex(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds:DataSet[(Long,String)] = env.fromElements("a","b","c","d","e").zipWithIndex
    ds.print()
  }

  /**
   * 生成唯一标识(索引),不一定连续，数据加工效率比 zipWitIndex 快
   */
  @Test
  def zipWithUniqueId(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds:DataSet[(Long,String)] = env.fromElements("a","b","c","d","e").zipWithUniqueId
    ds.print()
  }

  /**
   * 启动本地集群 createLocalEnvironment
   *
   * createRemoteEnvironment: 直接连接远程 job-manager，远程提交作业任务
   * getExecutionEnvironment： 如果是命令行运行就检测是否存在 fink 环境，有就直接子环境上运行，否则就在本地启动集群运行。不会启动 webUi.
   * createCollectionsEnvironment: 创建集合环境，适合嵌入 java 程序运行，接收简单 java 集合，然后处理，是单线程运行，因此非常轻量。
   *
   */
  @Test
  def createLocalEnvironment(): Unit ={
    val env = ExecutionEnvironment.createLocalEnvironment()
    env.fromElements("hello,python","scala,hello","python,hello")
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
  }

  @Test
  def createCollectionsEnvironment1(): Unit ={
    val env = ExecutionEnvironment.createCollectionsEnvironment
    env.fromElements("hello,python","scala,hello","python,hello")
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
  }

  /**
   * 借助 flink CollectiionEnv 实现 sql join 功能
   */
  @Test
  def createCollectionsEnvironment2(): Unit ={
    val env = ExecutionEnvironment.createCollectionsEnvironment
    val users = Seq(User(1, "Peter"), User(2, "John"), User(3, "Bill"))
    val emails = Seq(Email(1, "Re: Meeting", "How about 1pm?"),
      Email(1, "Re: Meeting", "Sorry, I'm not availble"),
      Email(3, "Re: Re: Project proposal", "Give me a few more days to think about it.")
    )

    val userDS = env.fromCollection(users)
    val emailDS = env.fromCollection(emails)

    val joinDS = userDS.join(emailDS).where(_.userIdentifier).equalTo(_.userId).map(j => (j._1,j._2))

    val result = new util.ArrayList[(User,Email)]

    joinDS.output(new LocalCollectionOutputFormat[(User,Email)](result));
    env.execute()

    result.foreach(println(_))
  }

  @Test
  def createRemoteEnvironment(): Unit ={
    val jarFiles = "/Users/huhao/softwares/idea_proj/flink-demo/official-tutorial/target/official-tutorial-jar-with-dependencies.jar"
    // 8081 是 jobmamager web ui 地址
    val env = ExecutionEnvironment.createRemoteEnvironment("hadoop01",8081,jarFiles)
    env.fromElements("hello,python","scala,hello","python,hello")
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
  }

}

