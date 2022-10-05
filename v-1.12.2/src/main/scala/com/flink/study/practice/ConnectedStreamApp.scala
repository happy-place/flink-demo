package com.flink.study.practice

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class WordFilter extends RichCoFlatMapFunction[String,String,String]{

  @transient private var isBlocked:ValueState[Boolean] = null

  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[Boolean]("isBlocked", classOf[Boolean])
    isBlocked = getRuntimeContext.getState(desc)
  }

  override def flatMap1(in1: String, collector: Collector[String]): Unit = {
    if(!isBlocked.value()){
      isBlocked.update(true)
    }
  }

  override def flatMap2(in2: String, collector: Collector[String]): Unit = {
    if(!isBlocked.value()){
      collector.collect(in2)
    }
  }
}

// Connected Stream 的实质是 一个KeydState 被两个流共享
object ConnectedStreamApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val func = new KeySelector[String,String]{
      override def getKey(in: String): String = in
    }
    val ctrl = env.fromElements("DROP", "IGNORE")keyBy(func)
    val streamOfWords = env.fromElements("Apache","Flink","DROP","IGNORE","Spring").keyBy(func)
    ctrl.connect(streamOfWords).flatMap(new WordFilter).print()

    env.execute("connected-stream")
  }

}
