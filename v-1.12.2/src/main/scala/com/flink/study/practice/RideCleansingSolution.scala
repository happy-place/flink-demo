package com.flink.study.practice

import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.ExerciseBase.printOrTest
import org.apache.flink.training.exercises.common.utils.GeoUtils

object RideCleansingSolution {

  /**
   * 找出起止都在伦敦的运程信息
   * @param args
   */
  def main(args: Array[String]) {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    // get the taxi ride data stream
    val rides = env.addSource(new TaxiRideGenerator())

    val filteredRides = rides
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))

    // print the filtered stream
    printOrTest(filteredRides)

    // run the cleansing pipeline
    env.execute("Taxi Ride Cleansing")
  }

}