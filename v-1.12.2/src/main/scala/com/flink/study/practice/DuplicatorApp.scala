package com.flink.study.practice

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.util.Collector

class Duplicator extends RichFlatMapFunction[TaxiRide,Long]{
  // 通过设置TtlConfig或注册定时器进行声明周期管理
  @transient private var isExisted:ValueState[Boolean]=null

  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[Boolean]("is-existed", classOf[Boolean])
    isExisted = getRuntimeContext.getState(desc)
  }

  override def flatMap(in: TaxiRide, collector: Collector[Long]): Unit = {
    val bool = isExisted.value() // 默认为false
    if(!bool){
      isExisted.update(true)
      collector.collect(in.driverId)
    }
  }

  override def close(): Unit = super.close()
}


object DuplicatorApp {

  // keyBy之后的状态都是KeyedState，none-keyed state通常鉴于source、sink中，自定义函数中不常见
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new TaxiRideGenerator()) // 仅使用数据源，无任何业务含义
      .keyBy(_.driverId)
      .flatMap(new Duplicator)
      .print()
    env.execute("duplicate")
  }

}