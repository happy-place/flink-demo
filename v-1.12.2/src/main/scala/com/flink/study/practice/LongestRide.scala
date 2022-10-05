package com.flink.study.practice

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.GeoUtils
import org.apache.flink.util.Collector

object LongestRide {


  /**
   * 实时计算耗时
   * @param args
   */
  def main(args: Array[String]) {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // get the taxi ride data stream
    val rides = env.addSource(new TaxiRideGenerator())

    val filteredRides = rides.flatMap(new RichFlatMapFunction[TaxiRide,(Int,Long)] {
      override def flatMap(in: TaxiRide, collector: Collector[(Int, Long)]): Unit = {
        if(!in.isStart){
          val cellId = GeoUtils.mapToGridCell(in.startLon, in.startLat)
          val time = in.endTime.toEpochMilli - in.startTime.toEpochMilli
          collector.collect((cellId,time))
        }
      }
    })

    filteredRides.keyBy(_._1)
      .maxBy(1) // 用到了隐式状态 key state
      .print()

    env.execute("Taxi Ride Max time cost")
  }

}
