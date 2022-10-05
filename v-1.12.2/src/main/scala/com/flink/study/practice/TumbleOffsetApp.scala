package com.flink.study.practice

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration
import java.util.concurrent.TimeUnit


/**
 * 滚动窗口通过offset控制窗口便宜量
 * 默认偏移量为0，即首次启动，就近找整点
 * 6> (6000,2)
 * 6> (11000,2)
 * 6> (16000,1)
 * 6> (26000,1)
 */
object TumbleOffsetApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val list = List(5000L,1000L,12000,7000L,2000L,9000L,21000L)
    val ds = env.addSource(new DataSource(list))
    ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[Long](){
        override def extractTimestamp(t: Long, l: Long): Long ={
          t
        }
      })).map((_,1))
      .keyBy(_._2)
      .window(TumblingEventTimeWindows.of(Time.seconds(5),Time.seconds(1))) //
      .process(new ProcessWindowFunction[(Long,Int),(Long,Int),Int,TimeWindow](){
        override def process(key: Int, context: Context, elements: Iterable[(Long, Int)], out: Collector[(Long, Int)]): Unit = {
          val length = elements.toArray.length
          out.collect(context.window.getEnd,length)
        }
      }).print()

    env.execute("offset")
  }

}
