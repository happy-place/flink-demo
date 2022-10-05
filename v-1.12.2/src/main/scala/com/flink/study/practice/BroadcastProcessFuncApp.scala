package com.flink.study.practice

import org.apache.flink.api.common.state.{MapStateDescriptor}
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

class WordFilterProcess(broadcastStateDesc:MapStateDescriptor[String,String]) extends KeyedBroadcastProcessFunction[String,String,String,String]{

  override def processElement(word: String, readOnlyContext: KeyedBroadcastProcessFunction[String, String, String, String]#ReadOnlyContext,
                              collector: Collector[String]): Unit = {
    // 读取广播流
    val blackWords = readOnlyContext.getBroadcastState(broadcastStateDesc)
    if(!blackWords.contains(word)){
      collector.collect(word)
    }
  }

  override def processBroadcastElement(black: String, context: KeyedBroadcastProcessFunction[String, String, String, String]#Context,
                                       collector: Collector[String]): Unit = {
    // 数据写入广播流吗，如果不写，就是空的
    val state = context.getBroadcastState(broadcastStateDesc)
    state.put(black,black)
  }
}

object BroadcastProcessFuncApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val words = env.socketTextStream("localhost",9000)

    // 广播流只能是 MapState 类型
    val desc = new MapStateDescriptor[String,String]("", classOf[String],classOf[String])
    val blacks = env.fromElements("app", "ios").broadcast(desc) // 将DataStream 转换为BroadcastStream

    words.keyBy((x)=>x).connect(blacks).process(new WordFilterProcess(desc)).print()

    env.execute("exec")
  }

}
