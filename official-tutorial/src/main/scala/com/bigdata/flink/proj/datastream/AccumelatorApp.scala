package com.bigdata.flink.proj.datastream

import org.apache.flink.api.common.accumulators.{Accumulator, IntCounter}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.io.Serializable

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/7 3:39 下午 
 * @desc: 自定义累加器
 *
 */
object AccumelatorApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val accName = "count"
    env.fromElements((1L, 3L), (1L, 5L), (1L, 7L), (2L, 4L), (2L, 2L))
      .keyBy(0)
      .flatMap(new RichFlatMapFunction[(Long,Long),Long]() {
        private val counter:IntCounter = new IntCounter() // 声明累加器
        private var state:ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          getRuntimeContext.addAccumulator(accName,counter) // 注册累加器
          state = getRuntimeContext.getState(new ValueStateDescriptor("sum",classOf[Long],0L))
        }

        override def flatMap(in: (Long, Long), collector: Collector[Long]): Unit = {
          counter.add(1) // 使用累加器
          val acc = state.value()+in._2
          state.update(acc)
          collector.collect(acc)
        }
      }).print()

    val result = env.execute("accumulator")
    val acc = result.getAccumulatorResult[Int](accName) // 获取累加器结果
    println(s"acc: ${acc}")
  }

  type MyInt = Int with Serializable

  class MyAccumulator extends Accumulator[MyInt,MyInt]{

    private var localValue = 0

    override def add(v: MyInt): Unit = localValue + v

    override def getLocalValue: MyInt = localValue.asInstanceOf[MyInt]

    override def resetLocal(): Unit = {
      localValue = 0
    }

    override def merge(accumulator: Accumulator[MyInt, MyInt]): Unit = {
      localValue += accumulator.getLocalValue
    }
  }

}


