package com.flink.study.practice

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.util.Collector

/**
 * 使用侧输出流过滤出符合指定条件的记录，避免fliter操作不必要的元素复制压力
 * 且侧输出类型可以与主输出类型不同
 */
object SideOutputApp1 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val outputTag = new OutputTag[String]("ou-shu")
    val ds = env.fromElements(2L,1L,3L,10L,1L,7L,14L,9L).process(new ProcessFunction[Long,Long](){
      override def processElement(i: Long, context: ProcessFunction[Long, Long]#Context, collector: Collector[Long]): Unit = {
        if(i%2==0){
          context.output(outputTag,i+"")
        }
        collector.collect(i)
      }
    })

    val ouShu = ds.getSideOutput(outputTag)

    ouShu.print("ou")
    ds.print("all")

    env.execute("sideoutput")
  }

}
