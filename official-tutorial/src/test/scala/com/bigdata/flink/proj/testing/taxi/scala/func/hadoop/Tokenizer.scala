package com.bigdata.flink.proj.testing.taxi.scala.func.hadoop

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/15 4:15 下午 
 * @desc:
 *
 */

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{JobConf, Mapper, OutputCollector, Reporter}

class Tokenizer extends Mapper[LongWritable, Text, Text, LongWritable] {
  override def map(k1: LongWritable, v1: Text, outputCollector: OutputCollector[Text, LongWritable],
                   reporter: Reporter): Unit = {
    v1.toString.split(",").foreach(word =>
      outputCollector.collect(new Text(word),new LongWritable(1L))
    )
  }

  override def configure(jobConf: JobConf): Unit = {

  }

  override def close(): Unit = {

  }
}
