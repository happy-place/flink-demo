package com.bigdata.flink.proj.testing.taxi.scala.func.hadoop

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/15 4:14 下午 
 * @desc:
 *
 */

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{JobConf, OutputCollector, Reducer, Reporter}

import java.util
import scala.collection.convert.ImplicitConversions.`iterator asScala`

class Counter extends Reducer [Text, LongWritable, Text, LongWritable] {
  override def reduce(k2: Text, iterator: util.Iterator[LongWritable], outputCollector: OutputCollector[Text, LongWritable],
                      reporter: Reporter): Unit = {
    val value = iterator.map(_.get()).sum
    outputCollector.collect(k2,new LongWritable(value))
  }

  override def configure(jobConf: JobConf): Unit = {

  }

  override def close(): Unit = {

  }
}