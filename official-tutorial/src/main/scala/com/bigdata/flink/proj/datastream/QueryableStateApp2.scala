package com.bigdata.flink.proj.datastream

import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.queryablestate.client.QueryableStateClient
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util.concurrent.TimeUnit

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/7 10:55 上午 
 * @desc:
 *
 */
object QueryableStateApp2 {

  def main(args: Array[String]): Unit = {
    val jobId = JobID.fromHexString(args(0))

    val stateDesc = new ValueStateDescriptor("num-sum",createTypeInformation[(Long,Long)])
    val stateQueryName = "myQueryableState"

    val client = new QueryableStateClient("localhost", 9069)

    try{
      val resultFuture = client.getKvState(jobId, stateQueryName, 1L, TypeInformation.of(classOf[Long]), stateDesc)
      resultFuture.thenAccept{resp =>
        println(resp.value())
      }
    }catch {
      case e:Exception =>
        TimeUnit.SECONDS.sleep(2)
    }

  }
}
