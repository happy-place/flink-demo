package com.bigdata.flink.proj.datastream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/8 9:58 上午 
 * @desc:
 *
 */
object FeedbackStreamDemo {

  def main(args: Array[String]): Unit = {
    // environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)

//    val source = env.addSource(new SimpleStringSource)
    val source = env.socketTextStream("localhost",9000)

    val mapStream = source.map(str => {
      val arr = str.split(",")
      println("map : " + str)
      (arr(0), arr(1).toLong)
    }).disableChaining()

    val itStrema = mapStream.iterate(ds => {
      // 迭代过程
      val dsMap = ds.map(str => {
        (str._1, str._2 + 1)
      })
        .keyBy(_._1)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .process(new ProcessWindowFunction[(String, Long), (String, Long), String, TimeWindow] {
          override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[(String, Long)]): Unit = {
            // process 简单的窗口求和
            val it = elements.toIterator
            var sum = 0l
            while (it.hasNext) {
              val current = it.next()
              sum = sum + current._2
            }
            out.collect(key, sum)
          }
        })

      // 反馈分支：窗口输出数据小于 500，反馈到 mapStream，重新窗口求和
      (dsMap.filter(s => {
        s._2 < 500
      })
        ,
        // 输出分支：大于等于 500 的就处理完了，直接输出
        dsMap.filter(s => {
          s._2 >= 500
        })
      )
    })
      .disableChaining() // 禁用算子链后，就看不到迭代环

    itStrema.print("result:")
    env.execute("FeedbackStreamDemo")
  }
}