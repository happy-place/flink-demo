package com.flink.study.practice

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.training.exercises.common.datatypes.TaxiFare
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator
import org.apache.flink.util.Collector

/**
 * 滚动窗口统计1h内各司机消费累计情况
 * 然后全局窗口统计top1
 */
object HourlyTipsSolution {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tipsOfDriver = env.addSource(new TaxiFareGenerator)
      .keyBy(_.driverId) // 统计1h内各司机的小费累计和
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .reduce(new ReduceFunction[TaxiFare](){ // ReduceFunction 进行预聚合，ProcessWindowFunction 混入窗口信息
        override def reduce(t: TaxiFare, t1: TaxiFare): TaxiFare = {
          t.tip = t.tip + t1.tip
          t
        }
      },new ProcessWindowFunction[TaxiFare,(Long,Long,Double),Long,TimeWindow](){
        override def process(key: Long, context: Context, elements: Iterable[TaxiFare],
                             out: Collector[(Long, Long, Double)]): Unit = {
          val fare = elements.iterator.next()
          val start = context.window.getStart
          out.collect((start,key,fare.tip))
        }
      })

    tipsOfDriver.print("driver")
    // 统计相同1h内，所有小费和最高的司机，此处窗口与上面窗口时同一个起止时间段，是对上面数据在全局视角的汇总
    val maxTips = tipsOfDriver
      .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
      .maxBy(2)

    maxTips.print("global")

    env.execute("global tip top2 in every hour")
  }

}
