package com.flink.study.practice

import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.datatypes.{TaxiFare, TaxiRide}
import org.apache.flink.training.exercises.common.sources.{TaxiFareGenerator, TaxiRideGenerator}
import org.apache.flink.util.Collector


/**
 * fare 关联 ride，先到的消息，立刻开始计时，在60s内对方为到，则报警
 * 借助旁路输出+定时器机制进行报警
 * @param emitAfter
 * @param outputTag
 */
class CoProcessFunc(emitAfter:Time,outputTag: OutputTag[String]) extends KeyedCoProcessFunction[Long,TaxiFare,TaxiRide,String]{

  @transient private var fareState:ValueState[TaxiFare] = null
  @transient private var rideState:ValueState[TaxiRide] = null

  @transient private var timerState:ValueState[Long] = null

  override def open(parameters: Configuration): Unit = {
    val config = StateTtlConfig.newBuilder(Time.days(1)).cleanupFullSnapshot().neverReturnExpired().build()
    val fareDesc = new ValueStateDescriptor[TaxiFare]("fare-state", classOf[TaxiFare])
    fareDesc.enableTimeToLive(config)
    val rideDesc = new ValueStateDescriptor[TaxiRide]("ride-state", classOf[TaxiRide])
    rideDesc.enableTimeToLive(config)
    val timerDesc = new ValueStateDescriptor[Long]("timer-state", classOf[Long])
    timerDesc.enableTimeToLive(config)
    fareState = getRuntimeContext.getState(fareDesc)
    rideState = getRuntimeContext.getState(rideDesc)
    timerState = getRuntimeContext.getState(timerDesc)
  }

  override def processElement1(fare: TaxiFare, context: KeyedCoProcessFunction[Long, TaxiFare, TaxiRide, String]#Context,
                               collector: Collector[String]): Unit = {
    val ride = rideState.value()
    if(ride!=null){
      collector.collect(s"${ride.driverId} ${ride.taxiId} ${fare.totalFare}")
      cleanUp(context)
    }else{
      fareState.update(fare)
      val eventTime = fare.getEventTime
      val emitAt = eventTime + emitAfter.toMilliseconds
      context.timerService().registerEventTimeTimer(emitAt)
    }
  }

  override def processElement2(ride: TaxiRide, context: KeyedCoProcessFunction[Long, TaxiFare, TaxiRide, String]#Context,
                               collector: Collector[String]): Unit = {
    val fare = fareState.value()
    if(fare!=null){
      collector.collect(s"${ride.driverId} ${ride.taxiId} ${fare.totalFare}")
      cleanUp(context)
    }else{
      rideState.update(ride)
      val eventTime = ride.getEventTime
      val emitAt = eventTime + emitAfter.toMilliseconds
      context.timerService().registerEventTimeTimer(emitAt)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[Long, TaxiFare, TaxiRide, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val fare = fareState.value()
    val ride = rideState.value()
    if((fare!=null && ride==null) || (fare==null && ride!=null)){
      ctx.output(outputTag,s"${fare} ${ride}")
    }
  }

  private def cleanUp(context: KeyedCoProcessFunction[Long, TaxiFare, TaxiRide, String]#Context): Unit ={
    rideState.clear()
    fareState.clear()
    val triggleAt = timerState.value()
    if(triggleAt!=0){
      context.timerService().deleteEventTimeTimer(triggleAt)
    }
    timerState.clear()
  }
}

object CoProcessFuncApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fares = env.addSource(new TaxiFareGenerator).keyBy(_.rideId)
    val rides = env.addSource(new TaxiRideGenerator).keyBy(_.rideId)

    val outputTag = new OutputTag[String]("join failed")

    val joined = fares.connect(rides).process(new CoProcessFunc(Time.minutes(1),outputTag))

    val joinFailed = joined.getSideOutput(outputTag)
    joined.print("joined")
    joinFailed.print("failed")

    env.execute("co-process")
  }

}
