package com.flink.study.practice

import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.datatypes.{TaxiFare, TaxiRide}
import org.apache.flink.training.exercises.common.sources.{TaxiFareGenerator, TaxiRideGenerator}
import org.apache.flink.util.Collector


class RideConnectFareFunc extends RichCoFlatMapFunction[TaxiRide,TaxiFare,String]{

  private var rideState:ValueState[TaxiRide] = null
  private var fareState:ValueState[TaxiFare] = null

  override def open(parameters: Configuration): Unit = {
    val ttlConfig = StateTtlConfig.newBuilder(Time.days(1)).neverReturnExpired().useProcessingTime().build()
    val rideDesc = new ValueStateDescriptor[TaxiRide]("ride-state", classOf[TaxiRide])
    rideDesc.enableTimeToLive(ttlConfig)
    val fareDesc = new ValueStateDescriptor[TaxiFare]("fare-state", classOf[TaxiFare])
    fareDesc.enableTimeToLive(ttlConfig)
    rideState = getRuntimeContext.getState(rideDesc)
    fareState = getRuntimeContext.getState(fareDesc)
  }

  override def flatMap1(ride: TaxiRide, collector: Collector[String]): Unit = {
    val fare = fareState.value()
    if(fare!=null){
      collector.collect(s"${ride.taxiId} ${fare.totalFare}")
    }else{
      rideState.update(ride)
    }
  }

  override def flatMap2(fare: TaxiFare, collector: Collector[String]): Unit = {
    val ride = rideState.value()
    if(ride!=null){
      collector.collect(s"${ride.taxiId} ${fare.totalFare}")
    }else{
      fareState.update(fare)
    }
  }
}

object RidesAndFaresSolution {
  /**
   * 通过 connected stream 拼接流，使用生命周期管理状态，inner join 逻辑
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fareStream = env.addSource(new TaxiFareGenerator).keyBy(_.rideId)
    val rideStream = env.addSource(new TaxiRideGenerator).keyBy(_.rideId)

    rideStream.connect(fareStream).flatMap(new RideConnectFareFunc).print()
    env.execute("Ride & Fare")
  }

}
