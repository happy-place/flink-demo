package com.flink.study.practice

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{OutputTag, _}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration
import java.util.concurrent.TimeUnit


class CountProcessFunc(outputTag: OutputTag[(Long,Int)]) extends ProcessWindowFunction[(Long,Int),(Long,Long,Int),Int,TimeWindow](){
  override def process(key: Int, context: Context, elements: Iterable[(Long, Int)], out: Collector[(Long,Long, Int)]): Unit = {
    var acc = 0;
    elements.foreach{x=>
      if(x._1<context.currentWatermark){
        context.output(outputTag,x)
      }else{
        acc+=1
      }
    }
    out.collect(context.window.getEnd,key,acc)
  }
}

class DataSource[T](list:List[T]) extends SourceFunction[T]{

  private var isRunning:Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[T]): Unit = {
    while(isRunning){
      for(num<-list){
        sourceContext.collect(num)
        TimeUnit.SECONDS.sleep(1)
      }
      isRunning = false
    }
  }

  override def cancel(): Unit = {
    isRunning = false;
  }
}


/**
 * 设置allowedLateness 同时设置sideOutputLateData，会自动将超过 watermark + lateness 的延迟数据发送到侧输出流
 */
object SideOutputApp2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val list = List(5000L,1000L,12000,7000L,2000L,9000L,21000L)
    val ds = env.addSource(new DataSource(list))
    val outputTag = new OutputTag[(Long,Int)]("late")
    val reducedStream = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[Long](){
        override def extractTimestamp(t: Long, l: Long): Long ={
          t
        }
      })).map((_,1))
      .keyBy(_._2)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .allowedLateness(Time.seconds(1))
      .sideOutputLateData(outputTag) // 设置
      .reduce(new ReduceFunction[(Long,Int)](){
        override def reduce(t: (Long, Int), t1: (Long, Int)): (Long, Int) = {
          (t._1,t._2+t1._2)
        }
      })

    val late = reducedStream.getSideOutput(outputTag)

    late.print("late")
    reducedStream.print("red")

    env.execute("sideoutput")
  }

}
