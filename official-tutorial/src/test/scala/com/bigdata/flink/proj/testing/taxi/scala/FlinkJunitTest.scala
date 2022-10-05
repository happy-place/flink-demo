package com.bigdata.flink.proj.testing.taxi.scala

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flink.util.Collector
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalamock.scalatest.MockFactory

import java.util
import java.util.Collections


/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/9 8:18 下午 
 * @desc:
 *
 */

class IncrementMapFunction extends MapFunction[Int, Int] {

  override def map(record: Int): Int = {
    record + 1
  }
}

// 测试 MapFunction 函数
class IncrementMapFunctionTest extends FlatSpec with Matchers {

  "IncrementMapFunction" should "increment values" in {
    // instantiate your function
    val incrementer: IncrementMapFunction = new IncrementMapFunction()

    // call the methods that you have implemented
    incrementer.map(2) should be(3)
  }

}

class IncrementFlatMapFunction extends FlatMapFunction[Int, Int] {
  override def flatMap(t: Int, collector: Collector[Int]): Unit = {
    collector.collect(t)
  }
}

class IncrementFlatMapFunctionTest extends FlatSpec with MockFactory {

  "IncrementFlatMapFunction" should "increment values" in {
    // instantiate your function
    val flattenFunction: IncrementFlatMapFunction = new IncrementFlatMapFunction()

    val collector = mock[Collector[Int]]

    //verify collector was called with the right output
    //    (collector.collect _).expects(3)

    // call the methods that you have implemented
    flattenFunction.flatMap(2, collector)
  }
}

class StreamingJobIntegrationTest extends FlatSpec with Matchers with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(2)
    .build)

  before {
    flinkCluster.before()
  }

  after {
    flinkCluster.after()
  }


  "IncrementFlatMapFunction pipeline" should "incrementValues" in {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure your test environment
    env.setParallelism(2) // 2个 taskmanager 每个 1 个 slot 或 1 个 taskmanager 每个 2 和 slot

    // values are collected in a static variable
    CollectSink.values.clear()

    // create a stream of custom elements and apply transformations
    env.fromElements(1, 21, 22).map(new IncrementMapFunction())
      .addSink(new CollectSink())

    // execute
    env.execute()

    // verify your results
    CollectSink.values should contain allOf (2, 22, 23)
  }
}
// create a testing sink  半生类对象
class CollectSink extends SinkFunction[Int] {

  override def invoke(value: Int): Unit = {
    CollectSink.values.add(value)
  }
}

object CollectSink {
  // must be static
  val values: util.List[Int] = Collections.synchronizedList(new util.ArrayList())
}