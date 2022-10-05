package com.flink.study.practice

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

case class SensorRecord(id:Int,ts:Long,value:Double)

class MaxReduce extends ReduceFunction[SensorRecord]{
  override def reduce(t1: SensorRecord, t2: SensorRecord): SensorRecord = {
    if(t1.value>t2.value){
      t1
    }else{
      t2
    }
  }
}

class MaxProcess extends ProcessWindowFunction[SensorRecord,(Int,Long,SensorRecord),Int,TimeWindow](){
  override def process(key: Int, context: Context, elements: Iterable[SensorRecord],
                       out: Collector[(Int, Long, SensorRecord)]): Unit = {
    val max = elements.iterator.next()
    out.collect((key,context.window.getEnd,max))
  }
}

object IncrementAggApp {
  // 增量聚合
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(SensorRecord(1,1,36.5),SensorRecord(2,1,36.1),SensorRecord(1,2,36.6),SensorRecord(2,4,36.0),SensorRecord(1,6,36.3)
      ,SensorRecord(2,6,36.3),SensorRecord(2,7,36.2))
    ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(5))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorRecord](){
        override def extractTimestamp(t: SensorRecord, l: Long): Long = t.ts
      })).keyBy(_.id)
      .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
      .reduce(new MaxReduce,new MaxProcess)
      .print()
    env.execute("acc")
  }
}
//6> (1,5,SensorRecord(1,2,36.6))
//8> (2,5,SensorRecord(2,1,36.1))
//6> (1,10,SensorRecord(1,6,36.3))
//8> (2,10,SensorRecord(2,6,36.3))
