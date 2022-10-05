package com.flink.study.practice

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig}
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.windowing.time.{Time=>WTime}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.training.exercises.common.datatypes.TaxiFare
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator
import org.apache.flink.util.Collector

/**
 * 使用定时器，模拟滚动窗口聚合，并支持处理迟到数据
 */
class TipSumProcessFunc(duration:Time) extends KeyedProcessFunction[Long,TaxiFare,(Long,Long,Double)]{

  // 可能同时计算多个窗口
  @transient private var sumOfTips:MapState[Long,Double] = null

  override def open(parameters: Configuration): Unit = { // 最后一条记录出现24小时内，仍保留数据
    val desc = new MapStateDescriptor[Long,Double]("tips", classOf[Long],classOf[Double])
    val ttlConfig = StateTtlConfig.newBuilder(Time.days(1)).cleanupFullSnapshot().neverReturnExpired().updateTtlOnCreateAndWrite().build()
    desc.enableTimeToLive(ttlConfig)
    sumOfTips = getRuntimeContext.getMapState(desc)
  }

  override def processElement(fare: TaxiFare, context: KeyedProcessFunction[Long, TaxiFare, (Long, Long, Double)]#Context,
                              collector: Collector[(Long, Long, Double)]): Unit = {
    val watermark = context.timerService().currentWatermark()
    val eventTime = fare.getEventTime
    val windowMillis = duration.toMilliseconds
    val windowEnd = eventTime - (eventTime%windowMillis) + windowMillis - 1

    val alreadyRegisted = sumOfTips.contains(windowEnd)
    val sum = (if(alreadyRegisted) sumOfTips.get(windowEnd) else 0.0F) + fare.tip

    sumOfTips.put(windowEnd,sum)

    if(eventTime <= watermark){ // 延迟数据，直接合并输出
      collector.collect((windowEnd,fare.driverId,sum))
    }else{ // 否则在定时器中输出
      if(!alreadyRegisted){
        context.timerService().registerEventTimeTimer(windowEnd) // 只在首条注册一次
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, TaxiFare, (Long, Long, Double)]#OnTimerContext,
                       out: Collector[(Long, Long, Double)]): Unit = {
    val driverId = ctx.getCurrentKey
    val tips = sumOfTips.get(timestamp)
    out.collect((timestamp,driverId,tips))
  }

}

object HourlyTipsSolution2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.addSource(new TaxiFareGenerator)
      .keyBy(_.driverId) // 统计1h内各司机的小费累计和
      .process(new TipSumProcessFunc(Time.hours(1)))
      .windowAll(TumblingEventTimeWindows.of(WTime.hours(1)))
      .maxBy(2)
      .print()

    env.execute("global tip top2 in every hour")
  }

}
