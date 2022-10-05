package com.bigdata.flink.proj.testing.taxi.scala

import com.bigdata.flink.proj.common.func.StreamSourceMock
import com.bigdata.flink.proj.taxi.bean.EnrichRide
import org.apache.flink.api.common.accumulators.{Histogram, IntCounter}
import org.apache.flink.api.common.{ExecutionConfig, RuntimeExecutionMode}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, RichCoFlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.runtime.operators.window.CountWindow
import org.apache.flink.table.runtime.operators.window.assigners.WindowAssigner
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.GeoUtils
import org.apache.flink.util.{Collector, SplittableIterator}
import org.joda.time.{Interval, Minutes}
import org.junit.Test

import java.time.Duration
import java.util
import java.util.Date
import scala.collection.convert.ImplicitConversions.`iterator asScala`

class RideTest {

  @Test
  def map(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new TaxiRideGenerator)
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
      .map(new EnrichRide(_)) // 辅助构造器创建对象需要new,主构造器构造对象不需要new
      .print()
    env.execute("map")
  }

  // flatMap默认用法还拿到集合，然后以元素为单位向外分发
  @Test
  def flatMap1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements("hello,hi", "jack,hello")
      .flatMap(_.split("\\,"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print()
    env.execute("map")
  }

  // flatMap实质定义的是，输入一个元素，允许多次输出元素，因此输入未必一定是集合
  @Test
  def flatMap2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new TaxiRideGenerator)
      .flatMap(new FlatMapFunction[TaxiRide, EnrichRide]() {
        override def flatMap(r: TaxiRide, out: Collector[EnrichRide]): Unit = {
          if (GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat)) {
            out.collect(new EnrichRide(r))
          }
        }
      }) // 辅助构造器创建对象需要new,主构造器构造对象不需要new
      .print()
    env.execute("map")
  }

  @Test
  def keyBy(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new TaxiRideGenerator)
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
      .flatMap(new FlatMapFunction[TaxiRide, (Int, Minutes)]() {
        override def flatMap(r: TaxiRide, out: Collector[(Int, Minutes)]): Unit = {
          val ride = new EnrichRide(r)
          val interval = new Interval(ride.startTime.toEpochMilli, ride.endTime.toEpochMilli)
          val minutes = interval.toDuration.toStandardMinutes
          out.collect((ride.startCell, minutes))
        }
      }) // 辅助构造器创建对象需要new,主构造器构造对象不需要new
      .keyBy(0)
      .maxBy(1)
      .print()
    env.execute("keyBy")
  }

  @Test
  def duplicate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new TaxiRideGenerator)
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
      .flatMap(new RichFlatMapFunction[TaxiRide, EnrichRide]() {

        private lazy val existedState = getRuntimeContext.getState(
          new ValueStateDescriptor("existed-state", classOf[Boolean]))

        override def flatMap(r: TaxiRide, out: Collector[EnrichRide]): Unit = {
          if (GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat)) {
            val existed = existedState.value()
            if (existed == null) {
              existedState.update(existed)
              out.collect(new EnrichRide(r))
            }
          }
        }
      })
      .print()
    env.execute("keyBy")
  }

  /**
   * 注：ListState，BroadcastState 尽管属于算子状态，但只能在 CheckpointedFunction 中使用，其余诸如 RichXxFunction中一律需要使用键控状态
   * flatMap1 与 flatMap2 是竞争关系，
   * 不借助广播变量的话，两个流必须keyBy，才能将配置和数据攒到一起
   */
  @Test
  def connect(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ignoreConfig = env.socketTextStream("localhost", 9000).keyBy(x => x)
    val dataDs = env.socketTextStream("localhost", 9001).keyBy(x => x)
    // 借助Keyby 将配置和数据攒到一块
    ignoreConfig.connect(dataDs).flatMap(new RichCoFlatMapFunction[String, String, String]() {
      private lazy val ignoreState = getRuntimeContext.getMapState(
        new MapStateDescriptor("ignore-set", classOf[String], classOf[String]))

      override def flatMap1(in1: String, collector: Collector[String]): Unit = {
        ignoreState.put(in1, null)
      }

      override def flatMap2(in2: String, collector: Collector[String]): Unit = {
        if (!ignoreState.contains(in2)) {
          collector.collect(in2)
        }
      }
    }).print()

    env.execute("connect")
  }

  @Test
  def connectBroadcast(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ignoreConfig = env.socketTextStream("localhost", 9000)
    val dataDs = env.socketTextStream("localhost", 9001)

    // ListState、BroadcastState 作为算子状态出现，避免 两个流都需要keyby
    val broadcastStateDesc = new MapStateDescriptor("ignore-state", classOf[String], classOf[String])
    val ignoreDS = ignoreConfig.broadcast(broadcastStateDesc)

    // 所有数据处理节点都获取一份广播数据
    dataDs.connect(ignoreDS).process(new BroadcastProcessFunction[String,String,String]() {
      override def processElement(in1: String, readOnlyContext: BroadcastProcessFunction[String, String, String]#ReadOnlyContext,
                                  collector: Collector[String]): Unit = {
        val ignores  = readOnlyContext.getBroadcastState(broadcastStateDesc)
        if(!ignores.contains(in1)){
          collector.collect(in1)
        }
      }

      override def processBroadcastElement(in2: String, context: BroadcastProcessFunction[String, String, String]#Context,
                                           collector: Collector[String]): Unit = {
        val ignores = context.getBroadcastState(broadcastStateDesc)
        ignores.put(in2,null)
      }
    }).print()

    env.execute("connect")
  }
  
  @Test
  def reduce(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(("sensor_1", 1607527992000L, 1),
      ("sensor_1", 1607527992050L, 1),
      ("sensor_2", 1607527992000L, 2),
      ("sensor_2", 1607527994000L, 3),
      ("sensor_1", 1607527994050L, 11),
      ("sensor_2", 1607527995500L, 5),
      ("sensor_2", 1607527995550L, 24),
      ("sensor_2", 1607527996000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))

    ds.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(
        new SerializableTimestampAssigner[(String,Long,Int)] {
          override def extractTimestamp(element: (String, Long, Int), recordTimestamp: Long): Long = element._2
        })
    ).keyBy(_._1)
      .timeWindow(Time.seconds(2))
      .reduce((t1,t2) => (t1._1,Long.MinValue,t1._3 + t2._3), // 增量计算
        new WindowFunction[(String,Long,Int),(String,Long,Int),String,TimeWindow](){
          override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long, Int)],
                             out: Collector[(String, Long, Int)]): Unit = {
            val tuple = input.iterator.next()
            out.collect((tuple._1,window.getStart,tuple._3)) // 裹上窗口时间
          }
        }
      ).print()

    env.execute("reduce")
  }

  @Test
  def processWindowFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(("sensor_1", 1607527992000L, 1),
      ("sensor_1", 1607527992050L, 1),
      ("sensor_2", 1607527992000L, 2),
      ("sensor_2", 1607527994000L, 3),
      ("sensor_1", 1607527994050L, 11),
      ("sensor_2", 1607527995500L, 5),
      ("sensor_2", 1607527995550L, 24),
      ("sensor_2", 1607527996000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))

    ds.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(
        new SerializableTimestampAssigner[(String,Long,Int)] {
          override def extractTimestamp(element: (String, Long, Int), recordTimestamp: Long): Long = element._2
        })
    ).keyBy(_._1)
      .timeWindow(Time.seconds(2))
      .process(new ProcessWindowFunction[(String,Long,Int),(String,Long,Int),String,TimeWindow](){ // 先憋住，然后一次性处理
        override def process(key: String, context: Context, elements: Iterable[(String, Long, Int)],
                             out: Collector[(String, Long, Int)]): Unit = {
          val head = elements.head
          val result = elements.map(_._3).sum
          out.collect((head._1,context.window.getStart,result))
        }
      }).print()

    env.execute("reduce")
  }

  @Test
  def async(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(("sensor_1", 1607527992000L, 1),
      ("sensor_1", 1607527992050L, 1),
      ("sensor_2", 1607527992000L, 2),
      ("sensor_2", 1607527994000L, 3),
      ("sensor_1", 1607527994050L, 11),
      ("sensor_2", 1607527995500L, 5),
      ("sensor_2", 1607527995550L, 24),
      ("sensor_2", 1607527996000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))

    ds.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(
        new SerializableTimestampAssigner[(String,Long,Int)] {
          override def extractTimestamp(element: (String, Long, Int), recordTimestamp: Long): Long = element._2
        })
    ).keyBy(_._1)
      .timeWindow(Time.seconds(2))
      .process(new ProcessWindowFunction[(String,Long,Int),(String,Long,Int),String,TimeWindow](){ // 先憋住，然后一次性处理
        override def process(key: String, context: Context, elements: Iterable[(String, Long, Int)],
                             out: Collector[(String, Long, Int)]): Unit = {
          val head = elements.head
          val result = elements.map(_._3).sum
          out.collect((head._1,context.window.getStart,result))
        }
      }).print()

    val client = env.executeAsync("reduce") // 提交运行就退出，不会阻塞至出结果，触发手动获取结果
    val jobId = client.getJobID
    val status = client.getJobStatus
    println(s"jobId: ${jobId}, job-status: ${status}")
//    val result = client.getJobExecutionResult.get()
//    println(result) // job 执行多长时间的汇总信息
  }


  @Test
  def iterations(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.generateSequence(0,5)
      .iterate{it =>
        val minusOne = it.map(_-1)
        (minusOne.filter(_<0),minusOne.filter(_>0))
      }.print() // 手动对流进行分拆，形成 tuple, _1 可以使用 SideOutput 输出，_2 直接从主流输出
    env.execute("iterations")
  }

  // 收集算子
  @Test
  def collect(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.generateSequence(0, 5)
    val iter = DataStreamUtils.collect(ds.javaStream)
    println(iter.mkString(","))
  }

  /**
   * Batch Mode 结果
   * 2> (java,2)
   *
   * Streaming Mode 结果
   * 2> (java,1)
   * 4> (hello,1)  <<< 为什么在 Batch mode 中丢了
   * 2> (java,2)
   * 4> (python,1) <<< 为什么在 Batch mode 中丢了
   *
   */
  @Test
  def batch(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    val ds = env.fromElements("hello,java","java,python")
    ds.flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()

    env.execute("batch")
  }

  /**
   * 每收集两个元素，计算一次平均值输出，然后清空状态重算
   * 1.对 Tuple 创建 ValueStateDescriptor 需要使用到 createTypeInformation[TupleStyle];
   * 2.每两个计算一次平均值，然后需要清空状态。
   */
  @Test
  def countWindowWithAverage(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements((1L, 3L), (1L, 5L), (1L, 7L), (1L, 4L), (1L, 2L))
    ds.keyBy(_._1)
      .flatMap(new RichFlatMapFunction[(Long,Long),(Long,Long)](){
        private var state:ValueState[(Long,Long)] = _

        override def open(parameters: Configuration): Unit = {
          state = getRuntimeContext.getState(new ValueStateDescriptor("acc-state",createTypeInformation[(Long,Long)]))
        }

        override def flatMap(in: (Long, Long), collector: Collector[(Long, Long)]): Unit = {
          val tuple = state.value()
          val newTup = if(tuple!=null){
            (tuple._1+1,tuple._2+in._2)
          }else{
            (1L,in._2)
          }
          state.update(newTup)
          if(newTup._1 >=2){
            collector.collect((in._1,newTup._2/newTup._1))
            state.clear()
          }
        }
      }).print()

    env.execute("countWindowWithAverage")
  }

  // 带状态相关算子
  @Test
  def mapWithState(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements((1L, 3L), (1L, 5L), (1L, 7L), (1L, 4L), (1L, 2L))
      .keyBy(0)
      .mapWithState{(in:(Long,Long),count:Option[Long]) =>
        count match {
          case Some(c) => ((in._1,c),Some(in._2 + c) )
          case None => ((in._1,0),Some(in._2))
        }
      }.print()
    env.execute("mapWithState")
  }

  @Test
  def accumulator(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val accName = "count"
    env.fromElements((1L, 3L), (1L, 5L), (1L, 7L), (2L, 4L), (2L, 2L))
      .keyBy(0)
      .flatMap(new RichFlatMapFunction[(Long,Long),Long]() {
        private val counter:IntCounter = new IntCounter() // 声明累加器
        private var state:ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          getRuntimeContext.addAccumulator(accName,counter) // 注册累加器
          state = getRuntimeContext.getState(new ValueStateDescriptor("sum",classOf[Long],0L))
        }

        override def flatMap(in: (Long, Long), collector: Collector[Long]): Unit = {
          counter.add(1) // 使用累加器
          val acc = state.value()+in._2
          state.update(acc)
          collector.collect(acc)
        }
      }).print()

    val result = env.execute("accumulator")
    val acc = result.getAccumulatorResult[Int](accName) // 获取累加器结果
    println(s"acc: ${acc}")
  }

  @Test
  def histogram(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val hisName = "count-his"
    env.fromElements((1L, 3L), (1L, 5L), (1L, 7L), (2L, 4L), (2L, 2L))
      .keyBy(0)
      .flatMap(new RichFlatMapFunction[(Long,Long),Long]() {
        private val his:Histogram = new Histogram() // 声明累加器
        private var state:ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          getRuntimeContext.addAccumulator(hisName,his)
          state = getRuntimeContext.getState(new ValueStateDescriptor("sum",classOf[Long],0L))
        }

        override def flatMap(in: (Long, Long), collector: Collector[Long]): Unit = {
          his.add(in._1.toInt) // 使用累加器，对谁进行累加，就传入谁
          val acc = state.value()+in._2
          state.update(acc)
          collector.collect(acc)
        }
      }).print()

    val result = env.execute("accumulator")
    val acc = result.getAccumulatorResult[util.TreeMap[Int,Int]](hisName) // 获取累加器结果
    println(s"acc: ${acc}")
  }

}
