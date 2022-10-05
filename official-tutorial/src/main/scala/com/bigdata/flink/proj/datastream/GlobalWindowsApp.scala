package com.bigdata.flink.proj.datastream

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/8 7:13 下午 
 * @desc:
 *
 */
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{PurgingTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, Window}


class MarksTrigger[W <: Window] extends Trigger[Marks,W] {

  override def onElement(element: Marks, timestamp: Long, window: W, ctx: TriggerContext): TriggerResult = {
    //trigger is fired if average marks of a student cross 80
    if(element.mark > 90) TriggerResult.FIRE
    else TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }
  override def onEventTime(time: Long, window: W, ctx: TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: W, ctx: TriggerContext) = ???
}

case class Marks(name : String, mark : Double, count : Int)

object GlobalWindowsApp {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost", 9999)

    // data is obtained in "name,mark" format
    val fdata = data.map { values =>
      val columns = values.split(",")
      (columns(0), columns(1).toDouble, 1)
    }

    // calculating average mark and number of exam attempts
    val keyed1 = fdata.keyBy(0).reduce { (x,y) =>
      (x._1, x._2 + y._2, x._3 + y._3)
    }.map( x => Marks(x._1, x._2 / x._3, x._3))


    val keyed = keyed1.keyBy(_.name)
      .window(GlobalWindows.create())
      .trigger(PurgingTrigger.of(new MarksTrigger[GlobalWindow]()))
      .maxBy(1)

    keyed.print()
    env.execute()

  }
}