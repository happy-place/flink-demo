package com.flink.study.practice

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * ReduceFunction、AggregateFunction 是增量计算，即来一条，合并一条
 * ProcessWindowFunction 是批处理方式，等窗口到期，一次性处理全部攒下的数据。<<< 存储消耗巨大
 * 或者两者合并使用，ReduceFunction 先进行预聚合，输入输出类型不用变，然后用ProcessWindowFunction读取已经被ReduceFunctio筛选出的一条记录，将其包装成其他类型输出
 *
 */
class SortFunc extends ProcessWindowFunction[(Long,Int),Long,Int, TimeWindow](){
  override def process(key: Int, context: Context, elements: Iterable[(Long, Int)],
                       out: Collector[Long]): Unit = {
    elements.toArray.sortBy(_._1).foreach(x=>out.collect(x._1))
  }
}

object WatermarkSortApp {

  // 将乱序数据校正顺序后再输出
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(2L,1L, 3L, 5L, 4L,6L, 8L, 7L, 9L, 12L, 10L)
    ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[Long](Duration.ofMillis(4))
      .withTimestampAssigner(new SerializableTimestampAssigner[Long]{
        override def extractTimestamp(t: Long, l: Long): Long = t
      })).map((_,1))
      .keyBy(_._2) // 针对tuple类型，使用x.y 方式，比指定下标准确
      .window(TumblingEventTimeWindows.of(Time.hours(1))) // window 和 windowAll 只能对 keyedStream使用
      .process(new SortFunc)
      .print()

    env.execute("data sort")
  }

}
//6> 1
//6> 2
//6> 3
//6> 4
//6> 5
//6> 6
//6> 7
//6> 8
//6> 9
//6> 10
//6> 12

