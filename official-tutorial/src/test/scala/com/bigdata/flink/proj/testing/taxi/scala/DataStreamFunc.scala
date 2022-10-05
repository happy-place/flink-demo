package com.bigdata.flink.proj.testing.taxi.scala

import com.bigdata.flink.proj.common.func.StreamSourceMock
import org.apache.flink.api.common.functions.{AggregateFunction, CoGroupFunction, FilterFunction, FlatMapFunction, JoinFunction, MapFunction, Partitioner, ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkStrategy}
import org.apache.flink.api.common.state.{BroadcastState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, CoProcessFunction, KeyedBroadcastProcessFunction, KeyedCoProcessFunction, ProcessJoinFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.scala.{createTypeInformation, _}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, EventTimeTrigger, PurgingTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow, Window}
import org.apache.flink.table.api.SessionWithGap

import java.io.{BufferedReader, File, FileInputStream, InputStream, InputStreamReader, PrintStream, Serializable}
import java.{lang, util}
import java.net.{ServerSocket, Socket}
import java.util.{Date, concurrent}
import java.util.concurrent.{Callable, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import scala.collection.convert.ImplicitConversions.`iterator asScala`
import scala.concurrent.ExecutionContext
import scala.util.Random

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/7 4:53 下午 
 * @desc:
 *
 */

case class T(id: String, tmstp: Long, vc: Int)

class FuncTest {

  @Test
  def map(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements(1, 2, 3).map(_ - 1).print()
    env.execute("map")
  }

  @Test
  def mapFunc(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements(1, 2, 3).map(new MapFunction[Int, String]() { // RichMapFunction
      override def map(t: Int): String = s"${t - 1}" // 没有 collector，且限制必须输出一个元素 (不能起到过滤作用)
    }).print()
    env.execute("map")
  }

  @Test
  def flatMap(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements("hello,java", "python,hello", "hello,python")
      .flatMap(_.split(",")) // 基于输入元素衍生出集合，然后将集合元素逐一输出
      .print()
    env.execute("map")
  }

  @Test
  def flatMapFunc(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements("hello,,java", "python,hello", "hello,python")
      .flatMap(new FlatMapFunction[String, String]() { // RichFlatMapFunction 常常可以当初 ProcessFunction 使用
        override def flatMap(t: String, collector: Collector[String]): Unit = {
          val arr = t.split(",")
          for (elem <- arr) {
            if (elem.size > 0) { // 有 collector，所可以进行过滤操作
              collector.collect(elem)
            }
          }
        }
      }) // 基于输入元素衍生出集合，然后将集合元素逐一输出
      .print()
    env.execute("map")
  }

  @Test
  def filter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements(1, 2, 3, 4, 5)
      .filter(_ % 2 > 0) // 输入元素，返回 boolean
      .print()
    env.execute("map")
  }

  @Test
  def filterFunc(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements(1, 2, 3, 4, 5)
      .filter(new FilterFunction[Int]() { // 无 collector，只能控制是否输出，不能控制输出什么，和多次输出
        override def filter(t: Int): Boolean = t % 2 > 0
      }) // 输入元素，返回 boolean
      .print()
    env.execute("map")
  }

  @Test
  def keyBy(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.fromElements("1,2", "1,2")
      .flatMap(_.split(","))
      .map((_, 1)) // 不光 kv，非 kv 也能
      .keyBy(0) // 下标、case class 字段名称
      .print()
    env.execute("map")
  }

  @Test
  def keyBy2(): Unit = {
    // 对 非 kv 进行 keyBy
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.fromElements("1,2", "1,2")
      .flatMap(_.split(","))
      .keyBy(i => i) // 下标、case class 字段名称
      .print()
    env.execute("map")
  }

  @Test
  def keyByFunc(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.fromElements("1,2", "1,2")
      .flatMap(_.split(","))
      .map((_, 1)) // 不光 kv，非 kv 也能
      .keyBy(new KeySelector[(String, Int), String]() {
        override def getKey(in: (String, Int)): String = in._1
      }) // 下标、case class 字段名称
      .print()
    env.execute("map")
  }

  @Test
  def reduce(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements("1,2", "1,2")
      .flatMap(_.split(","))
      .map((_, 1)) // 不光 kv，非 kv 也能
      .keyBy(0) // 下标、case class 字段名称
      .reduce((i1, i2) => (i1._1, i1._2 + i2._2)) // 聚合
      .print()
    env.execute("map")
  }

  @Test
  def reduceFunc(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements("1,2", "1,2")
      .flatMap(_.split(","))
      .map((_, 1)) // 不光 kv，非 kv 也能
      .keyBy(0) // 下标、case class 字段名称
      .reduce(new ReduceFunction[(String, Int)] { // RichStateFunction 有隐藏状态
        override def reduce(t: (String, Int), t1: (String, Int)): (String, Int) = (t._1, t._2 + t1._2)
      }) // 聚合
      .print()
    env.execute("map")
  }

  @Test
  def aggretations(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.fromElements(("a", 100), ("a", 99), ("a", 102), ("b", 101), ("b", 102))
      .keyBy(0) // 下标、case class 字段名称
      .maxBy(1) // min sum
      .print()
    env.execute("aggretations")
  }

  @Test
  def window(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      T("sensor_1", 1607527992000L, 1),
      T("sensor_1", 1607527992050L, 1),
      T("sensor_2", 1607527992000L, 2),
      T("sensor_2", 1607527993000L, 3),
      T("sensor_1", 1607527994050L, 11),
      T("sensor_2", 1607527995500L, 5),
      T("sensor_2", 1607527995550L, 24),
      T("sensor_2", 1607527997000L, 24))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[T](Duration.ofSeconds(2))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[T] {
          override def extractTimestamp(element: T, recordTimestamp: Long): Long = element.tmstp
        }
      ))

    dsWithTs.keyBy(_.id) // apply process 前面的 keyBy 如果是元祖的话，需要使用下划线表达式，使用字段名会导致编译错误
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply(new WindowFunction[T, T, String, TimeWindow]() {
        override def apply(key: String, window: TimeWindow, input: Iterable[T], out: Collector[T]): Unit = {
          input.foreach { in =>
            out.collect(T(in.id, window.getStart, in.vc))
          }
        }
      }).print()

    env.execute("reduce")
  }

  @Test
  def window2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      ("sensor_1", 1607527992000L, 1),
      ("sensor_1", 1607527992050L, 1),
      ("sensor_2", 1607527992000L, 2),
      ("sensor_2", 1607527993000L, 3),
      ("sensor_1", 1607527994050L, 11),
      ("sensor_2", 1607527995500L, 5),
      ("sensor_2", 1607527995550L, 24),
      ("sensor_2", 1607527997000L, 24))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[(String, Long, Int)](Duration.ofSeconds(2))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[(String, Long, Int)] {
          override def extractTimestamp(element: (String, Long, Int), recordTimestamp: Long): Long = element._2
        }
      ))

    dsWithTs.keyBy(_._1) // 注：此处必须以点方式明确key类型，否则后面是有ProcessWindowFunction会报类型问题type mismatch
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply(new WindowFunction[(String, Long, Int), (String, Long, Int), String, TimeWindow]() {
        override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long, Int)], out: Collector[(String, Long, Int)]): Unit = {
          input.foreach { in =>
            out.collect((in._1, window.getStart, in._3))
          }
        }
      }).print()

    env.execute("reduce")
  }


  @Test
  def windowReduce(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      T("sensor_1", 1607527992000L, 1),
      T("sensor_1", 1607527992050L, 1),
      T("sensor_2", 1607527992000L, 2),
      T("sensor_2", 1607527993000L, 3),
      T("sensor_1", 1607527994050L, 11),
      T("sensor_2", 1607527995500L, 5),
      T("sensor_2", 1607527995550L, 24),
      T("sensor_2", 1607527997000L, 24))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[T](Duration.ofSeconds(2))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[T] {
          override def extractTimestamp(element: T, recordTimestamp: Long): Long = element.tmstp
        }
      ))

    dsWithTs.keyBy(_.id) // apply 中慎用 元祖，会出现编译不通过问题
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .reduce(new ReduceFunction[T] { // 窗口聚合
        override def reduce(t: T, t1: T): T = T(t.id, t.tmstp, t.vc + t1.vc)
      }, new ProcessWindowFunction[T, T, String, TimeWindow]() { // 贴上窗口标签
        override def process(key: String, context: Context, elements: Iterable[T], out: Collector[T]): Unit = {
          val t = elements.iterator.next()
          out.collect(T(t.id, context.window.getStart, t.vc))
        }
      }).print()

    env.execute("reduce")
  }

  @Test
  def windowAgg(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      T("sensor_1", 1607527992000L, 1),
      T("sensor_1", 1607527992050L, 1),
      T("sensor_2", 1607527992000L, 2),
      T("sensor_2", 1607527993000L, 3),
      T("sensor_1", 1607527994050L, 11),
      T("sensor_2", 1607527995500L, 5),
      T("sensor_2", 1607527995550L, 24),
      T("sensor_2", 1607527997000L, 24))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[T](Duration.ofSeconds(2))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[T] {
          override def extractTimestamp(element: T, recordTimestamp: Long): Long = element.tmstp
        }
      ))

    dsWithTs.keyBy("id") // apply 中慎用 元祖，会出现编译不通过问题
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .sum("vc") // 窗口上聚合，求和之外字段，跟窗口最后一个元素一致
      .print()

    env.execute("reduce")
  }

  @Test
  def union(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.generateSequence(0, 3)
    val ds2 = env.generateSequence(3, 6)
    val ds3 = env.generateSequence(6, 9)
    ds1.union(ds2, ds3) // sql union all ，只能合并类型相同流
      .print()
    env.execute("reduce")
  }

  @Test
  def join(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val wms = WatermarkStrategy.forBoundedOutOfOrderness[T](Duration.ofSeconds(2))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[T] {
          override def extractTimestamp(element: T, recordTimestamp: Long): Long = element.tmstp
        }
      )

    val ds1 = env.fromElements(
      T("sensor_1", 1607527992000L, 1),
      T("sensor_2", 1607527991000L, 2),
      T("sensor_3", 1607527996000L, 3),
    ).assignTimestampsAndWatermarks(wms)

    val ds2 = env.fromElements(
      T("sensor_1", 1607527993000L, 10),
      T("sensor_3", 1607527994000L, 30),
      T("sensor_4", 1607527996000L, 40),
    ).assignTimestampsAndWatermarks(wms)

    // join 上的元素一个一个出
    ds1.join(ds2)
      .where(_.id).equalTo(_.id) // 窗口基础上求连表 (inner join)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(new JoinFunction[T, T, (T, T)]() {
        override def join(in1: T, in2: T): (T, T) = {
          (in1, in2)
        }
      }).print()
    env.execute("reduce")
  }

  @Test
  def coGroup1(): Unit = { // coGroup 实现 inner join
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val wms = WatermarkStrategy.forBoundedOutOfOrderness[T](Duration.ofSeconds(2))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[T] {
          override def extractTimestamp(element: T, recordTimestamp: Long): Long = element.tmstp
        }
      )

    val ds1 = env.fromElements(
      T("sensor_1", 1607527992000L, 1),
      T("sensor_2", 1607527994000L, 2),
      T("sensor_3", 1607527993000L, 3),
    ).assignTimestampsAndWatermarks(wms)

    val ds2 = env.fromElements(
      T("sensor_1", 1607527993000L, 10),
      T("sensor_3", 1607527992000L, 30),
      T("sensor_3", 1607527992000L, 31),
      T("sensor_4", 1607527996000L, 40),
    ).assignTimestampsAndWatermarks(wms)

    ds1.coGroup(ds2)
      .where(_.id).equalTo(_.id) //
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(
        new CoGroupFunction[T, T, (T, T)]() { // 先将左右都分别分组, 以组为单位将 key 相同的数据分别发往各自迭代器 iterable、iterable1，只匹配半边的也发
          override def coGroup(iterable: lang.Iterable[T], iterable1: lang.Iterable[T],
                               collector: Collector[(T, T)]): Unit = {
            val it1 = iterable.iterator()
            val it2 = iterable1.iterator()
            // inner join
            if (!it1.isEmpty && !it2.isEmpty) {
              it1.foreach { t1 =>
                it2.foreach(t2 => collector.collect(t1, t2))
              }
            }
          }
        }).print()
    env.execute("reduce")
  }

  @Test
  def coGroup2(): Unit = { // coGroup 实现 left join （right join）
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val wms = WatermarkStrategy.forBoundedOutOfOrderness[T](Duration.ofSeconds(2))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[T] {
          override def extractTimestamp(element: T, recordTimestamp: Long): Long = element.tmstp
        }
      )

    val ds1 = env.fromElements(
      T("sensor_1", 1607527992000L, 1),
      T("sensor_2", 1607527994000L, 2),
      T("sensor_3", 1607527993000L, 3),
    ).assignTimestampsAndWatermarks(wms)

    val ds2 = env.fromElements(
      T("sensor_1", 1607527993000L, 10),
      T("sensor_3", 1607527992000L, 30),
      T("sensor_3", 1607527992000L, 31),
      T("sensor_4", 1607527996000L, 40),
    ).assignTimestampsAndWatermarks(wms)

    ds1.coGroup(ds2)
      .where(_.id).equalTo(_.id) //
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(
        new CoGroupFunction[T, T, (T, T)]() { // 先将左右都分别分组, 以组为单位将 key 相同的数据分别发往各自迭代器 iterable、iterable1，只匹配半边的也发
          override def coGroup(iterable: lang.Iterable[T], iterable1: lang.Iterable[T],
                               collector: Collector[(T, T)]): Unit = {
            val it1 = iterable.iterator()
            val it2 = iterable1.iterator()
            // left join 同理 it1 与 it2 互换就是 right join
            if (!it1.isEmpty) {
              if (!it2.isEmpty) {
                it1.foreach { t1 =>
                  it2.foreach(t2 => collector.collect(t1, t2))
                }
              } else {
                it1.foreach { t1 =>
                  collector.collect(t1, null)
                }
              }
            }
          }
        }).print()
    env.execute("reduce")
  }

  @Test
  def coGroup3(): Unit = { // coGroup3 实现 full join
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val wms = WatermarkStrategy.forBoundedOutOfOrderness[T](Duration.ofSeconds(2))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[T] {
          override def extractTimestamp(element: T, recordTimestamp: Long): Long = element.tmstp
        }
      )

    val ds1 = env.fromElements(
      T("sensor_1", 1607527992000L, 1),
      T("sensor_2", 1607527994000L, 2),
      T("sensor_3", 1607527993000L, 3),
    ).assignTimestampsAndWatermarks(wms)

    val ds2 = env.fromElements(
      T("sensor_1", 1607527993000L, 10),
      T("sensor_3", 1607527992000L, 30),
      T("sensor_3", 1607527992000L, 31),
      T("sensor_4", 1607527996000L, 40),
    ).assignTimestampsAndWatermarks(wms)

    ds1.coGroup(ds2)
      .where(_.id).equalTo(_.id) //
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .apply(
        new CoGroupFunction[T, T, (T, T)]() { // 先将左右都分别分组, 以组为单位将 key 相同的数据分别发往各自迭代器 iterable、iterable1，只匹配半边的也发
          override def coGroup(iterable: lang.Iterable[T], iterable1: lang.Iterable[T],
                               collector: Collector[(T, T)]): Unit = {
            val it1 = iterable.iterator()
            val it2 = iterable1.iterator()
            // full join
            if (!it1.isEmpty) {
              if (!it2.isEmpty) {
                it1.foreach { t1 =>
                  it2.foreach(t2 => collector.collect(t1, t2))
                }
              } else {
                it1.foreach { t1 =>
                  collector.collect(t1, null)
                }
              }
            } else {
              it2.foreach { t2 =>
                collector.collect(null, t2)
              }
            }
          }
        }).print()
    env.execute("reduce")
  }

  // 连接两个 keyedStream，动态实现流拼接
  @Test
  def connect1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val wms = WatermarkStrategy.forBoundedOutOfOrderness[T](Duration.ofSeconds(2))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[T] {
          override def extractTimestamp(element: T, recordTimestamp: Long): Long = element.tmstp
        }
      )

    val ds1 = env.fromElements(
      T("sensor_1", 1607527992000L, 1),
      T("sensor_2", 1607527994000L, 2),
      T("sensor_3", 1607527993000L, 3),
    ).assignTimestampsAndWatermarks(wms).keyBy(_.id)

    val ds2 = env.fromElements(
      T("sensor_1", 1607527993000L, 10),
      T("sensor_3", 1607527992000L, 30),
      T("sensor_4", 1607527996000L, 40),
    ).assignTimestampsAndWatermarks(wms).keyBy(_.id)

    ds1.connect(ds2).process(new CoProcessFunction[T, T, (T, T)]() {
      private var left: MapState[String, T] = _
      private var right: MapState[String, T] = _

      override def open(parameters: Configuration): Unit = {
        left = getRuntimeContext.getMapState(new MapStateDescriptor("left-state", classOf[String], classOf[T]))
        right = getRuntimeContext.getMapState(new MapStateDescriptor("right-state", classOf[String], classOf[T]))
      }

      override def processElement1(in1: T, context: CoProcessFunction[T, T, (T, T)]#Context,
                                   collector: Collector[(T, T)]): Unit = {
        val t = right.get(in1.id)
        if (t != null) {
          collector.collect((in1, t))
          right.remove(in1.id)
        } else {
          left.put(in1.id, in1)
        }
      }

      override def processElement2(in2: T, context: CoProcessFunction[T, T, (T, T)]#Context,
                                   collector: Collector[(T, T)]): Unit = {
        val t = left.get(in2.id)
        if (t != null) {
          collector.collect((in2, t))
          left.remove(in2.id)
        } else {
          right.put(in2.id, in2)
        }
      }
    }).print()

    env.execute("reduce")
  }

  // 配置流广播给下游，指导数据加工，且配置流需要先加载
  @Test
  def connect2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val broadcastStateDesc = new MapStateDescriptor("config-state", classOf[String], classOf[String])
    val ds1 = env.socketTextStream("localhost", 9001)
    val ds2 = env.socketTextStream("localhost", 9002).broadcast(broadcastStateDesc)

    // 普通 stream 使用 BroadcastProcessFunction
    // keyedStream 使用 KeyedBroadcastProcessFunction
    ds1.connect(ds2).process(new BroadcastProcessFunction[String, String, String]() {
      override def processElement(in1: String, readOnlyContext: BroadcastProcessFunction[String, String, String]#ReadOnlyContext,
                                  collector: Collector[String]): Unit = {
        val state = readOnlyContext.getBroadcastState(broadcastStateDesc)
        if (!state.contains(in1)) {
          collector.collect(in1)
        }
      }

      override def processBroadcastElement(in2: String, context: BroadcastProcessFunction[String, String, String]#Context,
                                           collector: Collector[String]): Unit = {
        val state = context.getBroadcastState(broadcastStateDesc)
        state.put(in2, null)
      }
    }).print()

    env.execute("reduce")
  }

  @Test
  def coMap(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val ds1 = env.fromElements(0, 1, 2)
    val ds2 = env.fromElements("10", "11")
    // connect 连接两个结构不一样流，借助 coMap 统一成数据结构一样的流
    ds1.connect(ds2).map(
      (i: Int) => i,
      (j: String) => j.toInt
    ).print()

    env.execute("reduce")
  }

  @Test
  def coFlatMap(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val ds1 = env.fromElements(0, 1, 2)
    val ds2 = env.fromElements("10,11,12", "100,200")
    // connect 连接两个结构不一样流，借助 coFlapMap 扁平化成数据结构一样的流
    ds1.connect(ds2).flatMap(
      (i: Int) => Seq(i),
      (j: String) => j.split(",").map(_.toInt)
    ).print()

    env.execute("reduce")
  }

  // 自定义分区方式
  @Test
   def partitionCustom(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    env.generateSequence(0,6).map(i => (i,i)).partitionCustom(
      new Partitioner[Long](){
        override def partition(key: Long, numberOfPartition: Int): Int = (key % numberOfPartition).toInt
      },
      0).print()
    env.execute("partitionCustom")
   }

  /**
   * 上游算子 subtask 数据 随机发往下游，无固定对应关系。
   * 1 >> 6
   * 0 >> 2
   * 0 >> 3
   * 1 >> 4
   * 0 >> 0
   * 1 >> 5
   * 1 >> 1
   *
   * 随机分发
   *
   * 1> 2
   * 4> 3
   * 1> 0
   * 2> 1
   * 4> 4
   * 1> 6
   * 1> 5
   */
  @Test
  def shuffle(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.generateSequence(0,6).map(
      new RichMapFunction[Long,Long](){
        override def map(in: Long): Long = {
          val subtaskId = getRuntimeContext.getIndexOfThisSubtask
          println(s"${subtaskId} >> ${in}")
          in
        }
      }
    ).setParallelism(2)
      .shuffle
      .print().setParallelism(4)
    env.execute("partitionCustom")
  }

  /**
   * 上游算子 subtask 轮询发往下游。
   * map 并行度为 2
   * 0 >> 5
   * 1 >> 2
   * 1 >> 3
   * 0 >> 1
   * 0 >> 4
   *
   * 上游 0 发往 下游 1 2 4
   * 上游 1 发往 下游 2 3
   * 数据量足够情况下，上游 subtask 会发往下游所有 subtask
   * print 并行度为 4
   * 2> 2
   * 1> 1
   * 3> 3
   * 2> 4
   * 4> 5
   *
   * 注：没有设置并行的的算子，继承全局并行度
   */
  @Test
  def rebalance(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.generateSequence(1,5).map(
      // generateSequence 产生的是 Long, RichMapFunction 如果设置为 Int,出现 cannot resolve overload method map，原因是类型推断异常
      new RichMapFunction[Long,Long](){
        override def map(in: Long): Long = {
          val subtaskId = getRuntimeContext.getIndexOfThisSubtask
          println(s"${subtaskId} >> ${in}")
          in
        }
      }
    ).setParallelism(2)
      .rebalance
      .print()
      .setParallelism(4)
    env.execute("partitionCustom")
  }

  /**
   * 上游下游并行度存在倍数关系（上 2 下 4，或上 4 下 2)
   * 上下游 subtask 有固定搭配关系，以 上 2 下 4 为例，上游 0 发放下游 0、1， 上游 1 发往下游 2、3。
   * 上游 0 与 下游 0，1 的 subtask 运行在同一个 taskManager 上，数据在本地进行传输，不通过网络，相比于 rebalance 传输效率更高。
   *
   * 1 >> 2
   * 0 >> 5
   * 1 >> 4
   * 0 >> 1
   * 1 >> 3
   *
   * 上游 0 发往下游 1 2
   * 上游 1 发往下游 3 4
   *
   * 1> 5
   * 3> 2
   * 2> 1
   * 4> 4
   * 3> 3
   *
   */
  @Test
  def rescale(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.generateSequence(1,5).map(
      // generateSequence 产生的是 Long, RichMapFunction 如果设置为 Int,出现 cannot resolve overload method map，原因是类型推断异常
      new RichMapFunction[Long,Long](){
        override def map(in: Long): Long = {
          val subtaskId = getRuntimeContext.getIndexOfThisSubtask
          println(s"${subtaskId} >> ${in}")
          in
        }
      }
    ).setParallelism(2)
      .rescale
      .print()
      .setParallelism(4)
    env.execute("partitionCustom")
  }

  /**
   * 上游算子 subtask 数据发往下游算子 所有 subtask
   * 1 >> hello
   * 2 >> hello
   * 0 >> hello
   * 3 >> hello
   *
   * 广播流元素，被发往下游全部 subtask
   *
   */
  @Test
  def broadcast(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val brStateDesc = new MapStateDescriptor("broadcast-state",classOf[String],classOf[String])
    val ds1 = env.socketTextStream("localhost",9001).broadcast(brStateDesc)
    val ds2 = env.socketTextStream("localhost",9002)

    ds2.connect(ds1).process(new BroadcastProcessFunction[String,String,String]() {
      override def processElement(in1: String, readOnlyContext: BroadcastProcessFunction[String, String, String]#ReadOnlyContext,
                                  collector: Collector[String]): Unit = {

      }

      override def processBroadcastElement(in2: String, context: BroadcastProcessFunction[String, String, String]#Context,
                                           collector: Collector[String]): Unit = {
        val subtask = getRuntimeContext.getIndexOfThisSubtask
        println(s"${subtask} >> ${in2}")
      }
    })
    ds2.forward.print().setParallelism(2)
    env.execute("broadcast")
  }

  /**
   * slot
   * 1 >> 5
   * 0 >> 4
   * 1 >> 2
   * 1 >> 1
   * 1 >> 3
   * 1> 4
   * 2> 5
   * 2> 2
   * 2> 1
   * 2> 3
   *
   * 上游 0> 对应下游 1>
   * 同一个 slot 上游算子 subtask 的线程，直接发往下游算子 subtask 线程
   * 上下游并行度需要保持一致
   */
  @Test
  def forward(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.generateSequence(1,5).map(
      // generateSequence 产生的是 Long, RichMapFunction 如果设置为 Int,出现 cannot resolve overload method map，原因是类型推断异常
      new RichMapFunction[Long,Long](){
        override def map(in: Long): Long = {
          val subtaskId = getRuntimeContext.getIndexOfThisSubtask
          println(s"${subtaskId} >> ${in}")
          in
        }
      }
    ).setParallelism(2)
      .forward
      .print()
      .setParallelism(2)
    env.execute("partitionCustom")
  }

  /**
   * 上游算子所有 subtask 数据，统一发往下游第一个算子 subtask
   * 3 >> 2
   * 1 >> 1
   * 0 >> 4
   * 2 >> 5
   * 3 >> 3
   * 1> 2
   * 1> 3
   * 1> 1
   * 1> 5
   * 1> 4
   */
  @Test
  def global(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.generateSequence(1,5).map(
      // generateSequence 产生的是 Long, RichMapFunction 如果设置为 Int,出现 cannot resolve overload method map，原因是类型推断异常
      new RichMapFunction[Long,Long](){
        override def map(in: Long): Long = {
          val subtaskId = getRuntimeContext.getIndexOfThisSubtask
          println(s"${subtaskId} >> ${in}")
          in
        }
      }
    ).setParallelism(4)
      .global
      .print()
      .setParallelism(4)
    env.execute("partitionCustom")
  }

  /**
   * keyBy 相同的，交给同一个线程处理
   * 12> 5
   * 9> 4
   * 12> 2
   * 9> 1
   * 9> 0
   * 9> 3
   */
  @Test
  def keyBy1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.generateSequence(0,5)
      .keyBy(_ % 3)
      .print()
    env.execute("partitionCustom")
  }

  // 默认 flatMap filter map 符合 chain 条件，被连接到一起
  @Test
  def chain(): Unit ={
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.socketTextStream("localhost",9001)
      // 算子链 >>>
      .flatMap(_.split(","))
      .filter(_.startsWith("a"))
      .map((_,1))
      // 算子链 >>>
      .keyBy(0)
      .sum(1)

    env.execute("partitionCustom")
  }

  // filter 中间拦腰截断，禁止 chain
  @Test
  def disableChain(): Unit ={
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.socketTextStream("localhost",9001)
      .flatMap(_.split(","))
      .filter(_.startsWith("a")).disableChaining() // 拦腰斩断
      .map((_,1))
      .keyBy(0)
      .sum(1)

    env.execute("partitionCustom")
  }

  // filter 处手动创建新算子链
  @Test
  def startNewChain(): Unit ={
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.socketTextStream("localhost",9001)
      .flatMap(_.split(","))
      .filter(_.startsWith("a")).startNewChain()
      .map((_,1))
      .keyBy(0)
      .sum(1)

    env.execute("partitionCustom")
  }

  @Test
  def slotSharingGroup(): Unit ={
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.socketTextStream("localhost",9001)
      .flatMap(_.split(","))
      .filter(_.startsWith("a")).slotSharingGroup("new-group")
      .map((_,1))
      .keyBy(0)
      .sum(1)

    env.execute("partitionCustom")
  }

  /**
   * 形成算子链条件
   * 1.没有禁止算子链；
   * 2.没有在可以 chain 位置创建新的算子链；
   * 3.上下游算子并行度一致；
   * 4.上游算子链接策略为：Head（只能与下游链接，如 source) 或 Always（既能与上游链接、又能与下游链接)，下游算子链接策略为：Always；
   * 5.算子直接数据分发策略为 Forward （ForwardPartition：即上游发往下游数据是本地分发，不经过网络)；
   * 6.算子之间shuffle 不是批处理模式；
   *
   * 形成算子链好处：
   * 上下游算子各自多个 subtask 处在同一个 slot 时，原本一个 subtask 一个线程，chain 后，这些 subtask 共用一个线程，避免计算过程线程切换、
   * 数据在线程之间传递时缓冲、序列化开销。
   *
   */

  @Test
  def tumbling(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new SourceFunction[Int](){
      private var isRunning = true

      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        import org.apache.flink.streaming.api.watermark.Watermark
          while(isRunning){
            val i = Random.nextInt(100)
//            sourceContext.collect(i)
            val tmstp = System.currentTimeMillis()
            sourceContext.collectWithTimestamp(i,tmstp)
            sourceContext.emitWatermark(new Watermark(tmstp)) // 水印就是当前时间，不加延迟
            TimeUnit.SECONDS.sleep(1)
          }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

//    // keyed stream 求和
//    ds.map((_,1)).keyBy(_._1)
//      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//      .sum(1)
//      .print()
//
//    // 全局窗口，求最大只，注：使用 TumblingEventTimeWindows 无法解决隐式转换问题，报错
//    ds.timeWindowAll(Time.seconds(5))
//      .max(0)
//      .print()


//    ds.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
//      .max(0)
//      .print()

    ds.map((_,1)).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
      .max(0)
      .print()

    env.execute("tumbling")
  }

  @Test
  def test(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new SourceFunction[Int](){
      private var isRunning = true

      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        import org.apache.flink.streaming.api.watermark.Watermark
        while(isRunning){
          val i = Random.nextInt(100)
          //            sourceContext.collect(i)
          val tmstp = System.currentTimeMillis()
          sourceContext.collectWithTimestamp(i,tmstp)
          sourceContext.emitWatermark(new Watermark(tmstp)) // 水印就是当前时间，不加延迟
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

    //  全局窗口，求最大只，注：使用 TumblingEventTimeWindows 无法解决隐式转换问题，报错
    ds.timeWindowAll(Time.seconds(5))
      .max(0)
      .print()

//    ds.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
//      .max(0)
//      .print()

    ds.map((_,1)).windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
      .max(0)
      .print()

    env.execute("tumbling")
  }


  @Test
  def tumbling2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new SourceFunction[Int](){
      private var isRunning = true

      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        import org.apache.flink.streaming.api.watermark.Watermark
        while(isRunning){
          val i = Random.nextInt(100)
          //            sourceContext.collect(i)
          val tmstp = System.currentTimeMillis()
          sourceContext.collectWithTimestamp(i,tmstp) // TODO 定义数据源时就声明了时间和水印，后面可以不用 assignTimestampAndWatrmark
          sourceContext.emitWatermark(new Watermark(tmstp)) // 水印就是当前时间，不加延迟
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

    // keyed stream 能使用并行处理能力
    ds.map((_,1))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5),Time.seconds(2)))
      .sum(1)
      .print()

    // windowAll 貌似只能处理 tuple ，单值报错
//    ds.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
//      .max(0)
//      .print()

    // windowAll 全窗口，不能使用并行处理能力
    // TumblingEventTimeWindows(size,offset) offset 对齐默认为 0
    ds.map((_,1))
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(5),Time.seconds(2)))
      .process(new ProcessAllWindowFunction[(Int,Int),String,TimeWindow](){
        override def process(context: Context, elements: Iterable[(Int,Int)], out: Collector[String]): Unit = {
          elements.foreach{e =>
            out.collect(s"${e} ${new Date(context.window.getStart)} ${new Date(context.window.getEnd)}")
          }
        }
      }).print()

    // 时间窗口
    ds.timeWindowAll(Time.seconds(5))
      .max(0)
      .print()

    env.execute("tumbling")
  }

  @Test
  def sliding(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new SourceFunction[Int](){
      private var isRunning = true

      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        import org.apache.flink.streaming.api.watermark.Watermark
        while(isRunning){
          val i = Random.nextInt(100)
          //            sourceContext.collect(i)
          val tmstp = System.currentTimeMillis()
          sourceContext.collectWithTimestamp(i,tmstp) // TODO 定义数据源时就声明了时间和水印，后面可以不用 assignTimestampAndWatrmark
          sourceContext.emitWatermark(new Watermark(tmstp)) // 水印就是当前时间，不加延迟
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })


    // keyed stream 能使用并行处理能力
    ds.map((_,1))
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(2),Time.seconds(1)))
      .sum(1)
      .print()

    // SlidingEventTimeWindows(size,slide,offset) offset 对齐默认为 0
    ds.map((_,1))
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(5),Time.seconds(2),Time.seconds(1)))
      .process(new ProcessAllWindowFunction[(Int,Int),String,TimeWindow](){
        override def process(context: Context, elements: Iterable[(Int,Int)], out: Collector[String]): Unit = {
          elements.foreach{e =>
            out.collect(s"${e} ${new Date(context.window.getStart)} ${new Date(context.window.getEnd)}")
          }
        }
      }).print()

    // 时间窗口
    ds.timeWindowAll(Time.seconds(5),Time.seconds(2))
      .max(0)
      .print()

    env.execute("tumbling")
  }

  @Test
  def session(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new SourceFunction[Int](){
      private var isRunning = true
      private val random = new Random()

      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        import org.apache.flink.streaming.api.watermark.Watermark
        while(isRunning){
          val i = Random.nextInt(100)
          //            sourceContext.collect(i)
          val tmstp = System.currentTimeMillis()
          sourceContext.collectWithTimestamp(i,tmstp) // TODO 定义数据源时就声明了时间和水印，后面可以不用 assignTimestampAndWatrmark
          sourceContext.emitWatermark(new Watermark(tmstp)) // 水印就是当前时间，不加延迟
          TimeUnit.SECONDS.sleep(random.nextInt(5))
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

    // EventTimeSessionWindows.withGap(gap) offset 对齐默认为 0
    ds.map((_,1))
      .windowAll(EventTimeSessionWindows.withGap(Time.seconds(3)))
      .process(new ProcessAllWindowFunction[(Int,Int),String,TimeWindow](){
        override def process(context: Context, elements: Iterable[(Int,Int)], out: Collector[String]): Unit = {
          elements.foreach{e =>
            out.collect(s"${e} ${new Date(context.window.getStart)} ${new Date(context.window.getEnd)}")
          }
        }
      }).print()

    env.execute("tumbling")
  }

  /**
   * 自定义全局窗口，默认窗口结束时间或元素个数为 Long.Max_Value
   * 需要手动触发
   */
  @Test
  def globalWindow(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new SourceFunction[Int](){
      private var isRunning = true

      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        while(isRunning){
          val i = Random.nextInt(100)
                      sourceContext.collect(i)
//          val tmstp = System.currentTimeMillis()
//          sourceContext.collectWithTimestamp(i,tmstp) // TODO 定义数据源时就声明了时间和水印，后面可以不用 assignTimestampAndWatrmark
//          sourceContext.emitWatermark(new Watermark(tmstp)) // 水印就是当前时间，不加延迟
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

    ds.map((_,1)).keyBy(_._1)
      .window(GlobalWindows.create())
      .trigger(PurgingTrigger.of(new MyCountTrigger[GlobalWindow](3)))
      .sum(1)
      .print()

    env.execute("tumbling")
  }

  @Test
  def reduceFunction(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new SourceFunction[Int](){
      private var isRunning = true
      private val random = new Random()

      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        import org.apache.flink.streaming.api.watermark.Watermark
        while(isRunning){
          val i = Random.nextInt(5)
          //            sourceContext.collect(i)
          val tmstp = System.currentTimeMillis()
          sourceContext.collectWithTimestamp(i,tmstp) // TODO 定义数据源时就声明了时间和水印，后面可以不用 assignTimestampAndWatrmark
          sourceContext.emitWatermark(new Watermark(tmstp)) // 水印就是当前时间，不加延迟
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

    // 没有窗口，来一个处理一个
    /*
    ds.keyBy(i => i)
      .reduce(_ + _) // 必须是 keyBy 流
      .print()
     */

    ds.map((_,1)).keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .reduce((i1,i2) => (i1._1,i1._2 + i2._2))
      .print("no-processs")

    // reduce 后面跟 ProcessWindowFunction ，带上 window 信息
    ds.map((_,1)).keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .reduce((i1,i2) => (i1._1,i1._2+i2._2),
        new ProcessWindowFunction[(Int,Int),(Int,Int,Long),Int,TimeWindow](){
          override def process(key: Int, context: Context, elements: Iterable[(Int, Int)], // 迭代器对应 reduce 的输出
                               out: Collector[(Int, Int, Long)]): Unit = {
            val tuple = elements.iterator.next() // reduce 结果输出到这里
            out.collect((tuple._1,tuple._2,context.window.getStart))
          }
        }).print("has-process")


    env.execute("reduceFunction")
  }

  @Test
  def aggFunction(): Unit ={
    import org.apache.flink.streaming.api.watermark.Watermark
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new SourceFunction[Int](){
      private var isRunning = true
      private val random = new Random()

      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        while(isRunning){
          val i = Random.nextInt(5)
          //            sourceContext.collect(i)
          val tmstp = System.currentTimeMillis()
          sourceContext.collectWithTimestamp(i,tmstp) // TODO 定义数据源时就声明了时间和水印，后面可以不用 assignTimestampAndWatrmark
          sourceContext.emitWatermark(new Watermark(tmstp)) // 水印就是当前时间，不加延迟
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

    // 没有窗口，来一个处理一个
    /*
    ds.keyBy(i => i)
      .reduce(_ + _) // 必须是 keyBy 流
      .print()
     */

    val avgAgg = new AggregateFunction[(Int,Int),(Int,Int),Double](){
      override def createAccumulator(): (Int, Int) = (0,0)

      override def add(in: (Int, Int), acc: (Int, Int)): (Int, Int) = (acc._1+1,acc._2+in._2)

      override def getResult(acc: (Int, Int)): Double = acc._2 / (acc._1 * 1.0)

      override def merge(acc: (Int, Int), acc1: (Int, Int)): (Int, Int) = (acc._1 + acc1._1,acc._2 + acc1._2)
    }

    ds.map((_,1)).keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .aggregate(avgAgg)
      .print("no-processs")

    // reduce 后面跟 ProcessWindowFunction ，带上 window 信息
    ds.map((_,1)).keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .aggregate(avgAgg,
        new ProcessWindowFunction[Double,(Double,Long),Int,TimeWindow](){
          override def process(key: Int, context: Context, elements: Iterable[Double],
                               out: Collector[(Double, Long)]): Unit = {
            val avg = elements.iterator.next() // reduce 结果输出到这里
            out.collect((avg,context.window.getStart))
          }
        }).print("has-process")


    env.execute("reduceFunction")
  }

  @Test
  def processWindowFunc(): Unit ={
    import org.apache.flink.streaming.api.watermark.Watermark
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new SourceFunction[Int](){
      private var isRunning = true
      private val random = new Random()

      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        while(isRunning){
          val i = Random.nextInt(5)
          //            sourceContext.collect(i)
          val tmstp = System.currentTimeMillis()
          sourceContext.collectWithTimestamp(i,tmstp) // TODO 定义数据源时就声明了时间和水印，后面可以不用 assignTimestampAndWatrmark
          sourceContext.emitWatermark(new Watermark(tmstp)) // 水印就是当前时间，不加延迟
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

    // 没有窗口，来一个处理一个
    /*
    ds.keyBy(i => i)
      .reduce(_ + _) // 必须是 keyBy 流
      .print()
     */

    val avgAgg = new AggregateFunction[(Int,Int),(Int,Int),Double](){
      override def createAccumulator(): (Int, Int) = (0,0)

      override def add(in: (Int, Int), acc: (Int, Int)): (Int, Int) = (acc._1+1,acc._2+in._2)

      override def getResult(acc: (Int, Int)): Double = acc._2 / (acc._1 * 1.0)

      override def merge(acc: (Int, Int), acc1: (Int, Int)): (Int, Int) = (acc._1 + acc1._1,acc._2 + acc1._2)
    }

    ds.map((_,1)).keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(
        new ProcessWindowFunction[(Int,Int),(Double,Double,Long),Int,TimeWindow](){
          private lazy val stateDesc = new MapStateDescriptor("state-desc",classOf[Long],classOf[Double])
          private var state:MapState[Long,Double] = _

          override def open(parameters: Configuration): Unit = {
            state = getRuntimeContext.getMapState(stateDesc)
          }

          // 收集整个窗口元素，一次性处理
          override def process(key: Int, context: Context, elements: Iterable[(Int, Int)], out: Collector[(Double,Double, Long)]): Unit = {
            var sum = 0
            var count = 0
            elements.foreach{e =>
              sum += e._2
              count += 1
            }
            val key = sum/(count*1.0)
            val value = context.window.getStart
            state.put(value,key)

            val globalState = context.globalState.getMapState(stateDesc) // 通过 global 获取之前状态
            val prevStart = value - 10 * 1000
            val prevKey = if(globalState.contains(prevStart)){
              globalState.get(prevStart)
            }else{
              0L
            }
            out.collect((key,prevKey,value))
          }
        }).print()

    env.execute("reduceFunction")
  }

  @Test
  def windowFunc(): Unit ={
    import org.apache.flink.streaming.api.watermark.Watermark
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new SourceFunction[Int](){
      private var isRunning = true
      private val random = new Random()

      override def run(sourceContext: SourceFunction.SourceContext[Int]): Unit = {
        while(isRunning){
          val i = Random.nextInt(5)
          //            sourceContext.collect(i)
          val tmstp = System.currentTimeMillis()
          sourceContext.collectWithTimestamp(i,tmstp) // TODO 定义数据源时就声明了时间和水印，后面可以不用 assignTimestampAndWatrmark
          sourceContext.emitWatermark(new Watermark(tmstp)) // 水印就是当前时间，不加延迟
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        isRunning = false
      }
    })

    // 自定 windowFunction
    ds.map((_,1)).keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply(new WindowFunction[(Int,Int),(Double,Long),Int,TimeWindow]() {
        override def apply(key: Int, window: TimeWindow, elements: Iterable[(Int, Int)],
                           out: Collector[(Double, Long)]): Unit = {
          var sum = 0
          var count = 0
          elements.foreach{e =>
            sum += e._2
            count += 1
          }
          val key = sum/(count*1.0)
          val value = window.getStart
          out.collect((key,value))
        }
      }).print()
    env.execute("reduceFunction")
  }

  /**
   * ok:7> (1607527990000,1,1)
   * ok:7> (1607527990000,2,2)
   * ok:7> (1607527990000,3,3)
   * late:7> (sensor_1,1607527991000,1)
   * ok:7> (1607527995000,2,2)
   * ok:7> (1607528000000,2,2)
   *
   * 1.水印 = 元素时间 - 水印延迟，且水印计算维持只增不减逻辑；
   * 2.当前元素计算完水印后，如果水印位置 > 最近窗口最大时间戳(x999为边界),则触发窗口计算。
   * 3.窗口触发后，迟到元素不会导致水印增长，因此可以源源不断触发窗口计算
   * 4.窗口触发后，如果新元素加入，导致水印增长，在满足 之前触发计算窗口最大时间戳(x999 结尾) + 迟到容忍 < 水印位置调节下，依旧能够接受迟到数据，否则就不能处理，交给侧端输出
   *
   */
  @Test
  def allowLateness(): Unit ={
    import org.apache.flink.streaming.api.watermark.Watermark
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val seq = Seq(
      ("sensor_1", 1607527992000L, 1), // w=1607527990000L 水印 = t - 水印延迟
      ("sensor_1", 1607527997000L, 1), // w=1607527995000L (w 1607527995000L > win_max 1607527994999L 首次触发窗口)
      ("sensor_1", 1607527990000L, 1), // w=1607527995000L 水印只增不减(t < w 但 w 1607527995000L < win_end 1607527994999L + late 2000L 水印小于元素归属窗口最大时间戳 + 延迟容忍，仍可以接收处理 )
      ("sensor_1", 1607527991000L, 1), // w=1607527995000L (t < w 但 w 1607527995000L < win_end 1607527994999L + late 2000L 水印小于元素归属窗口最大时间戳 + 延迟容忍，仍可以接收处理 )
      ("sensor_1", 1607527999000L, 1), // w=1607527997000L
      ("sensor_1", 1607527991000L, 1), // w=1607527997000L (t < w 但 w 1607527997000L > win_end 1607527994999L + late 2000L 水印 不小于 元素归属窗口最大时间戳 + 延迟容忍，因此不能处理了 )
      ("sensor_1", 1607528000000L, 1),
      ("sensor_1", 1607528002000L, 1)
    )

    val ds = env.addSource(new StreamSourceMock[(String,Long,Int)](seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Long,Int)]() {
        override def extractTimestamp(t: (String, Long, Int), l: Long): Long = t._2
      }))

    val lateTag = new OutputTag[(String, Long, Int)]("late")

    // 自定 windowFunction
    val ds2 = ds.keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .allowedLateness(Time.seconds(2))
      .sideOutputLateData(lateTag)
      .process(new ProcessWindowFunction[(String,Long,Int),(Long,Int,Int),String,TimeWindow]() {
        override def process(key: String, context: Context, elements: Iterable[(String,Long,Int)],
                             out: Collector[(Long, Int,Int)]): Unit = {
          var sum = 0
          var count = 0
          elements.foreach{e =>
            sum += e._3
            count += 1
          }
          out.collect((context.window.getStart,sum,count))
        }
      })

    ds2.print("ok")
    ds2.getSideOutput(lateTag).print("late")

    env.execute("reduceFunction")
  }

  /**
   * 滚动窗口 Join
   * 9> (0,0,0),(0,0,0)
   * 6> (1,1,1),(1,1,1)
   * 4> (3,3,3),(3,3,3)
   * 1> (4,4,4),(4,4,4)
   */
  @Test
  def tumblingWindowJoin(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val seq1 = Seq(
      ("0", 0L, 0),
      ("1", 1L, 1),
      ("3", 3L, 3),
      ("4", 4L, 4),
    )

    val seq2 = Seq(
      ("0", 0L, 0),
      ("1", 1L, 1),
      ("2", 2L, 2),
      ("3", 3L, 3),
      ("4", 4L, 4),
      ("5", 5L, 5),
      ("6", 6L, 6),
      ("7", 7L, 7),
    )

    val wms = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Long,Int)]() {
        override def extractTimestamp(t: (String, Long, Int), l: Long): Long = t._2
      })

    val ds1 = env.addSource(new StreamSourceMock[(String,Long,Int)](seq1,false)).assignTimestampsAndWatermarks(wms).keyBy(_._1)
    val ds2 = env.addSource(new StreamSourceMock[(String,Long,Int)](seq2,false)).assignTimestampsAndWatermarks(wms).keyBy(_._1)

    ds1.join(ds2)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(2)))
      .apply((t1,t2) => s"${t1},${t2}")
      .print()

    env.execute("reduceFunction")
  }

  /**
   * 滑动窗口 join
   * 9> (0,0,0),(0,0,0)
   * 9> (0,0,0),(0,0,0)
   * 4> (3,3,3),(3,3,3)
   * 1> (4,4,4),(4,4,4)
   * 4> (3,3,3),(3,3,3)
   * 1> (4,4,4),(4,4,4)
   */
  @Test
  def slidingWindowJoin(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val seq1 = Seq(
      ("0", 0L, 0),
      ("3", 3L, 3),
      ("4", 4L, 4),
    )

    val seq2 = Seq(
      ("0", 0L, 0),
      ("1", 1L, 1),
      ("2", 2L, 2),
      ("3", 3L, 3),
      ("4", 4L, 4),
    )

    val wms = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Long,Int)]() {
        override def extractTimestamp(t: (String, Long, Int), l: Long): Long = t._2
      })

    val ds1 = env.addSource(new StreamSourceMock[(String,Long,Int)](seq1,false)).assignTimestampsAndWatermarks(wms).keyBy(_._1)
    val ds2 = env.addSource(new StreamSourceMock[(String,Long,Int)](seq2,false)).assignTimestampsAndWatermarks(wms).keyBy(_._1)

    ds1.join(ds2)
      .where(_._1)
      .equalTo(_._1)
      .window(SlidingEventTimeWindows.of(Time.milliseconds(2),Time.milliseconds(1)))
      .apply((t1,t2) => s"${t1},${t2}")
      .print()

    env.execute("reduceFunction")
  }

  /**
   * 会话窗口
   * 8> (5,5,5),(5,5,5)
   */
  @Test
  def sessionWindowJoin(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val seq1 = Seq(
      ("0", 0L, 0),
      ("4", 4L, 4),
      ("5", 5L, 5),
    )

    val seq2 = Seq(
      ("1", 1L, 1),
      ("2", 2L, 2),
      ("5", 5L, 5),
      ("6", 6L, 6),
      ("8", 8L, 8),
      ("9", 9L, 9),
    )

    val wms = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Long,Int)]() {
        override def extractTimestamp(t: (String, Long, Int), l: Long): Long = t._2
      })

    val ds1 = env.addSource(new StreamSourceMock[(String,Long,Int)](seq1,false)).assignTimestampsAndWatermarks(wms).keyBy(_._1)
    val ds2 = env.addSource(new StreamSourceMock[(String,Long,Int)](seq2,false)).assignTimestampsAndWatermarks(wms).keyBy(_._1)

    ds1.join(ds2)
      .where(_._1)
      .equalTo(_._1)
      .window(EventTimeSessionWindows.withGap(Time.milliseconds(1)))
      .apply((t1,t2) => s"${t1},${t2}")
      .print()

    env.execute("reduceFunction")
  }

  @Test
  def intervalJoin(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val seq1 = Seq(
      ("0", 0L, 0),
      ("1", 1L, 1),
      ("6", 6L, 6),
      ("7", 7L, 7),
    )

    val seq2 = Seq(
      ("0", 0L, 0),
      ("2", 2L, 2),
      ("3", 3L, 3),
      ("4", 4L, 4),
      ("5", 5L, 5),
      ("7", 7L, 7),
    )

    val wms = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Long,Int)]() {
        override def extractTimestamp(t: (String, Long, Int), l: Long): Long = t._2
      })

    val ds1 = env.addSource(new StreamSourceMock[(String,Long,Int)](seq1,false)).assignTimestampsAndWatermarks(wms).keyBy(_._1)
    val ds2 = env.addSource(new StreamSourceMock[(String,Long,Int)](seq2,false)).assignTimestampsAndWatermarks(wms).keyBy(_._1)

    ds1.intervalJoin(ds2)
      .between(Time.milliseconds(-2),Time.milliseconds(1))
//      .lowerBoundExclusive() // 默认左闭右闭，通过设置 xxxBoundExclusive 打开边界
//      .upperBoundExclusive()
      .process(new ProcessJoinFunction[(String,Long,Int),(String,Long,Int),String](){
        override def processElement(in1: (String, Long, Int), in2: (String, Long, Int),
                                    context: ProcessJoinFunction[(String, Long, Int), (String, Long, Int), String]#Context,
                                    collector: Collector[String]): Unit = {
          val elemTmstp = context.getTimestamp
          val leftTmstp = context.getLeftTimestamp
          val righTmstp = context.getRightTimestamp

          collector.collect(s"${in1},${in2} ${elemTmstp} ${leftTmstp} ${righTmstp}")
        }
      })
      .print()

    env.execute("reduceFunction")
  }

  /**
   * 两个流 join 拼接 ，超过 5 毫秒 未 join 成功发送报警
   * 9> (0,0,0),(0,0,0)
   * 11> (7,7,7),(7,7,7)
   * late:6> ds1: (1,1,1)
   * 6> (1,1,1),(1,8,8)
   * late:3> ds2: (2,2,2)
   * late:2> ds1: (6,6,6)
   * late:8> ds2: (5,5,5)
   * late:4> ds2: (3,3,3)
   * late:1> ds2: (4,4,4)
   */
  @Test
  def coJoinByProcessFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val seq1 = Seq(
      ("0", 0L, 0),
      ("1", 1L, 1),
      ("6", 6L, 6),
      ("7", 7L, 7),
    )

    val seq2 = Seq(
      ("0", 0L, 0),
      ("2", 2L, 2),
      ("3", 3L, 3),
      ("4", 4L, 4),
      ("5", 5L, 5),
      ("7", 7L, 7),
      ("1", 8L, 8),
    )

    val wms = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(0))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Long,Int)]() {
        override def extractTimestamp(t: (String, Long, Int), l: Long): Long = t._2
      })

    val ds1 = env.addSource(new StreamSourceMock[(String,Long,Int)](seq1,false)).assignTimestampsAndWatermarks(wms).keyBy(_._1)
    val ds2 = env.addSource(new StreamSourceMock[(String,Long,Int)](seq2,false)).assignTimestampsAndWatermarks(wms).keyBy(_._1)

    val lateTag = new OutputTag[String]("late")

    val lateAlertMills = 5

    val ds = ds1.connect(ds2)
      .process(new KeyedCoProcessFunction[String,(String,Long,Int),(String,Long,Int),String](){

        private var state1:MapState[String,(String,Long,Int)] = _
        private var state2:MapState[String,(String,Long,Int)] = _

        private var timerState:ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          state1 = getRuntimeContext.getMapState(new MapStateDescriptor("state1",classOf[String],classOf[((String,Long,Int))]))
          state2 = getRuntimeContext.getMapState(new MapStateDescriptor("state2",classOf[String],classOf[((String,Long,Int))]))
          timerState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer",classOf[Long]))
        }
        // processElement 和 onTimer 是串行调用，不存在并发问题
        override def processElement1(in1: (String, Long, Int),
                                     context: KeyedCoProcessFunction[String, (String, Long, Int), (String, Long, Int), String]#Context,
                                     collector: Collector[String]): Unit = {
          val key = in1._1
          if(state2.contains(key)){
            collector.collect(s"${in1},${state2.get(key)}")
            state2.remove(key)
            context.timerService().deleteEventTimeTimer(timerState.value())
          }else{
            state1.put(key,in1)
            val timerTmstp = in1._2 + lateAlertMills
            timerState.update(timerTmstp)
            context.timerService().registerEventTimeTimer(timerTmstp)
          }
        }

        override def processElement2(in2: (String, Long, Int),
                                     context: KeyedCoProcessFunction[String, (String, Long, Int), (String, Long, Int), String]#Context,
                                     collector: Collector[String]): Unit = {
          val key = in2._1
          if(state1.contains(key)){
            collector.collect(s"${state1.get(key)},${in2}")
            state1.remove(key)
            context.timerService().deleteEventTimeTimer(timerState.value())
          }else{
            state2.put(key,in2)
            val timerTmstp = in2._2 + lateAlertMills
            timerState.update(timerTmstp)
            context.timerService().registerEventTimeTimer(timerTmstp)
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[String, (String, Long, Int), (String, Long, Int), String]#OnTimerContext,
                             out: Collector[String]): Unit = {
          val key = ctx.getCurrentKey
          if(state1.contains(key)){
            ctx.output(lateTag,s"ds1: ${state1.get(key)}")
          }else{
            ctx.output(lateTag,s"ds2: ${state2.get(key)}")
          }
        }
      })
    ds.print()
    ds.getSideOutput(lateTag).print("late")
    env.execute("coJoinByProcessFunc")
  }

  def readData(inputStream:InputStream, charset:String = "UTF-8"): String ={
    var len = 0
    val bytes = new Array[Byte](1024)
    val stringBuilder = new StringBuilder()
    while((len = inputStream.read(bytes)) != -1){
      stringBuilder.append(bytes,0,len,charset)
    }
    stringBuilder.toString()
  }

  /**
   * 1.导包问题 AsyncFunction 必须是 org.apache.flink.streaming.api.scala.async.AsyncFunction，否则 unorderedWait 编译报错；
   * 2.AsyncFunction 中涉及的异步数据源客户端必须能够序列化，使用 object 封装查询逻辑，就可以解决；
   */
  @Test
  def unorderedWait1(): Unit ={
    // 服务端
    val port = 9001
    new Thread(new Runnable {
      override def run(): Unit = {
        SocketUtil.socketServer(port)
      }
    }).start()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds = env.fromElements(1, 2, 3)
    // 必须是 org.apache.flink.streaming.api.scala.async.AsyncFunction，导错包，unorderedWait 飘红报错
    val asyncFunc = new AsyncFunction[Int,String](){
      implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())
      override def asyncInvoke(input: Int, resultFuture: ResultFuture[String]): Unit = {
        val future = SocketUtil.asyncSocketClient("localhost", port, input)
        future.onSuccess{
          case result:String => resultFuture.complete(Seq(result))
        }
      }
    }
    // timeout 保证调用不超时，避免一直返回，导致任务中断
    AsyncDataStream.unorderedWait(ds, asyncFunc, 100L, TimeUnit.SECONDS,100).print()
    env.execute("asyncIO")
  }

  /**
   * 1.使用 async io 解决数据查询问题，被查询数据源如果支持异步客户端访问：即返回 Future 对象，则直接调用 AsyncFunction
   * 2.被查询数据源如果不支持异步客户端，则需要手动窗口线程池，以多线程方式访问，实现 RichAsyncFunction，在 open 和 close 方法中控制线程池声明周期
   */
  @Test
  def unorderedWait2(): Unit ={
    // 服务端
    val port = 9001
    new Thread(new Runnable {
      override def run(): Unit = {
        SocketUtil.socketServer(port)
      }
    }).start()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds = env.fromElements(1, 2, 3)
    // 必须是 org.apache.flink.streaming.api.scala.async.RichAsyncFunction，导错包，unorderedWait 飘红报错
    val asyncFunc = new RichAsyncFunction[Int,String](){

      private var pool:ThreadPoolExecutor = _

      override def open(parameters: Configuration): Unit = {
        pool = new ThreadPoolExecutor(
          5,
          8,
          100,
          TimeUnit.MINUTES,
          new LinkedBlockingQueue[Runnable](Int.MaxValue)
        )
      }

      override def asyncInvoke(input: Int, resultFuture: ResultFuture[String]): Unit = {
        pool.submit(new Runnable {
          override def run(): Unit = {
            val result = SocketUtil.socketClient("localhost", port, input)
            resultFuture.complete(Seq(result))
          }
        })
      }

      override def close(): Unit = {
        pool.shutdown()
      }

    }
    // timeout 保证调用不超时，避免一直返回，导致任务中断
    AsyncDataStream.unorderedWait(ds, asyncFunc, 100L, TimeUnit.SECONDS,100).print()
    env.execute("asyncIO")
  }

  @Test
  def orderedWait(): Unit ={
    // 服务端
    val port = 9001
    new Thread(new Runnable {
      override def run(): Unit = {
        SocketUtil.socketServer(port)
      }
    }).start()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds = env.fromElements(1, 2, 3)
    // 必须是 org.apache.flink.streaming.api.scala.async.AsyncFunction，导错包，unorderedWait 飘红报错
    val asyncFunc = new AsyncFunction[Int,String](){
      implicit lazy val executor: ExecutionContext = ExecutionContext.fromExecutor(Executors.directExecutor())

      override def asyncInvoke(input: Int, resultFuture: ResultFuture[String]): Unit = {
        val future = SocketUtil.asyncSocketClient("localhost", port, input)
        future.onSuccess{
          case result:String => resultFuture.complete(Seq(result))
        }
      }
    }
    // timeout 保证调用不超时，避免一直返回，导致任务中断
    // capacity 并行处理能力
    AsyncDataStream.orderedWait(ds, asyncFunc, 100L, TimeUnit.SECONDS,100).print()
    env.execute("asyncIO")
  }

  def fromArgs(args:Array[String]): ParameterTool ={
    ParameterTool.fromArgs(args)
  }

  @Test
  def fromPropertiesFile(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val path = getClass.getClassLoader.getResource("args/test.properties").getPath
//    val tool = ParameterTool.fromPropertiesFile( path)

//    val file = new File(path)
//    val tool = ParameterTool.fromPropertiesFile( file)

//    val input = new FileInputStream(path)
//    val tool = ParameterTool.fromPropertiesFile( input)

    // JVM 参数使用 -Dxx=yy, main 参数使用 --xx yy
    val args = Array[String]("--host","localhost","--port","9001")
    val tool = fromArgs(args)

    val parameterNum = tool.getNumberOfParameters // 参数个数
    println(parameterNum)

    env.getConfig.setGlobalJobParameters(tool) // 存到 flink Job 环境中，然后在 context 中可以访问到

    val host = tool.getRequired("host")
    val port = tool.getInt("port",9001) // 默认值
    env.socketTextStream(host,port)
      .flatMap(new RichFlatMapFunction[String,String]() {
        override def open(parameters: Configuration): Unit = {
          val parameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
          println( parameters.getRequired("host"))
          println( parameters.getInt("port",9001))
        }

        override def flatMap(t: String, collector: Collector[String]): Unit = {
          t.split(",").foreach(collector.collect(_))
        }
      })
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()
    env.execute("fromPropertiesFile")
  }




}


// 手动编写 count trigger
class MyCountTrigger[W <: Window](val size:Int) extends Trigger[(Int,Int),W] {

  private var sum = 0

  override def onElement(element: (Int,Int), timestamp: Long, window: W, ctx: TriggerContext): TriggerResult = {
    if(sum == size) {
      TriggerResult.FIRE
    } else{
      sum += 1
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
  override def onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  @throws[Exception]
  override def clear(window: W, ctx: Trigger.TriggerContext) :Unit= {
    sum = 0
  }
}
