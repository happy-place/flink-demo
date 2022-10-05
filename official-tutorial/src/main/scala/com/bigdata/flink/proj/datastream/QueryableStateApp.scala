package com.bigdata.flink.proj.datastream

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{StateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.{ConfigConstants, Configuration, QueryableStateOptions}
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util.concurrent.{CompletableFuture, TimeUnit}

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/7 10:55 上午 
 * @desc:
 *
 */
object QueryableStateApp {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()

    conf.setBoolean("queryable-state.enable", true)
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val stateDesc = new ValueStateDescriptor("num-sum", createTypeInformation[(Long, Long)])
    val stateQueryName = "myQueryableState"

    env.socketTextStream("localhost", 9001)
      .flatMap(_.split(","))
      .map(i => (1L, i.toLong))
      .keyBy(_._1)
      .flatMap(new CountWindowAverage(stateDesc, stateQueryName))
      .print()

    val client = env.executeAsync("QueryableStateApp")
    println(client.getJobID.toHexString)
    client.getJobExecutionResult.get()
  }


  class CountWindowAverage(stateDesc: ValueStateDescriptor[(Long, Long)], stateQueryName: String) extends RichFlatMapFunction[(Long, Long), (Long, Long)] {
    private var state: ValueState[(Long, Long)] = _

    override def open(parameters: Configuration): Unit = {
      stateDesc.setQueryable(stateQueryName)
      state = getRuntimeContext.getState(stateDesc)
    }

    override def flatMap(in: (Long, Long), collector: Collector[(Long, Long)]): Unit = {
      val tuple = state.value()
      val newTuple = if (tuple == null) {
        (1L, in._2)
      } else {
        (tuple._1 + 1, tuple._2 + in._2)
      }

      if (newTuple._1 >= 2) {
        collector.collect((in._1, newTuple._2 / newTuple._1))
        state.clear()
      } else {
        state.update(newTuple)
      }
    }
  }

}
