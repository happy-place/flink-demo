package com.bigdata.flink.source

import com.bigdata.flink.func.StreamSourceMock
import com.bigdata.flink.model.WaterSensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.{$, call, lit}
import org.apache.flink.table.api.{FieldExpression, Tumble}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableAggregateFunction.RetractableCollector
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableAggregateFunction, TableFunction}
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration
import scala.collection.mutable.ListBuffer

class HashCode(factor:Int=13) extends ScalarFunction {
  def eval(value: String): Int ={
    value.hashCode * factor
  }
}

class Split(separator: String=",") extends TableFunction[(String,Int)]{
  def eval(value:String):Unit = {
    value.split(separator).foreach(seg => collect((seg,seg.size)))
  }
}

case class VCAccumulator(var sum:Int=0, var count:Int=0)

class AvgVC extends AggregateFunction[Double,VCAccumulator] {
  override def getValue(acc: VCAccumulator): Double = {
    if(acc.count==0){
      0.0
    }else{
      getRound(acc,3)
    }
  }

  override def createAccumulator(): VCAccumulator = new VCAccumulator

  def accumulate(acc: VCAccumulator, data:Int): Unit ={
    acc.count += 1
    acc.sum += data
  }

  def getRound(acc: VCAccumulator, c:Int): Double ={
    math.round(acc.sum / (acc.count * 1.0) * math.pow(10,c)) / math.pow(10,c)
  }
}

class WindowAvgVC extends AvgVC{

  def retract(acc: VCAccumulator, data:Int): Unit ={
    acc.count -= 1
    acc.sum -= data
  }

  def merge(acc: VCAccumulator, it: java.lang.Iterable[VCAccumulator]): Unit = {
    val iter = it.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.count += a.count
      acc.sum += a.sum
    }
  }

  def resetAccumulator(acc: VCAccumulator): Unit = {
    acc.count = 0
    acc.sum = 0
  }
}

/**
 * ??????????????????????????????
 *                     ?????????????????????    ??????????????????
 * TableAggregateFunction[(Int, Int), ListBuffer[Int]]
 * @param n
 * @param desc
 */
class TopN(n:Int=2,desc:Boolean=true) extends TableAggregateFunction[(Int, Int), ListBuffer[Int]] {

  override def createAccumulator(): ListBuffer[Int] = {
    val list = ListBuffer[Int]()
    for(i <- 0 until n){
      list.append(Int.MinValue)
    }
    list
  }

  def accumulate(acc: ListBuffer[Int], value: Int): Unit = {
    val factor = if(desc) 1 else -1
    if(value * factor > acc(n-1) * factor){
      acc.append(value)
    }
    val temp = acc.sortWith(_ * factor > _ * factor).take(n)
    acc.clear()
    acc.appendAll(temp)
  }

  def merge(acc: ListBuffer[Int], it: java.lang.Iterable[ListBuffer[Int]]) {
    val iter = it.iterator()
    while (iter.hasNext) {
      iter.next().foreach(accumulate(acc, _))
    }
  }

  def emitValue(acc: ListBuffer[Int], out: Collector[(Int, Int)]): Unit = {
    for(i <- 0 until acc.size){
      if(acc(i)!= Int.MinValue){
        out.collect((acc(i), i+1))
      }
    }
  }
}

/**
 * ?????????????????????topN??????
 *                      ??????:????????????     ????????????: ??????????????????
 * TableAggregateFunction[(Int, Int), ListBuffer[(Int,Int)]
 * @param n
 * @param desc
 */
class RetractableTopN(n:Int=2,desc:Boolean=true) extends TableAggregateFunction[(Int, Int), ListBuffer[(Int,Int)]] {

  override def createAccumulator(): ListBuffer[(Int,Int)] = {
    val list = ListBuffer[(Int,Int)]()
    for(i <- 0 until n){
      list.append((Int.MinValue,Int.MinValue))
    }
    list
  }

  def accumulate(acc: ListBuffer[(Int,Int)], value: Int): Unit = {
    val factor = if(desc) 1 else -1
    if(value * factor > acc(n-1)._1 * factor){
      acc.append((value,Int.MinValue))
    }
    val oldAcc = acc.take(n)
    val newAcc = acc.sortWith(_._1 * factor > _._1 * factor).take(n)

    val temp = oldAcc.zip(newAcc).map{tup =>
      val oldTup = tup._1
      val newTup = tup._2
      if(oldTup._1 != newTup._1){
        (newTup._1,oldTup._1)
      }else{
        newTup
      }
    }.toList

    acc.clear()
    acc.appendAll(temp)
  }

  def merge(acc: ListBuffer[(Int,Int)], it: java.lang.Iterable[ListBuffer[(Int,Int)]]) {
    val iter = it.iterator()
    while (iter.hasNext) {
      iter.next().foreach(i => accumulate(acc, i._1))
    }
  }

  def emitValue(acc: ListBuffer[(Int,Int)], out: Collector[(Int, Int)]): Unit = {
    for(i <- 0 until acc.size){
      if(acc(i)._1!= Int.MinValue){
        out.collect((acc(i)._1, i+1))
      }
    }
  }

  def emitUpdateWithRetract(acc: ListBuffer[(Int,Int)], out: RetractableCollector[(Int, Int)]): Unit = {
    for(i <- 0 until acc.size){
      val tup = acc(i)
      if(tup._1!=tup._2){
        if(tup._2!=Int.MinValue){
          out.retract((tup._2, i+1))
        }
        out.collect((tup._1, 1))
      }
    }
  }

}


class TabFunction {

  /**
   * UDF ????????????????????????????????????
   * +----+--------------------------------+-------------+
   * | op |                             id |        hash |
   * +----+--------------------------------+-------------+
   * | +I |                       sensor_1 |  -772373508 |
   * | +I |                       sensor_1 |  -772373508 |
   * | +I |                       sensor_2 |  -772373495 |
   */
  @Test
  def scalarFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq, false))
    val hashCode = new HashCode()
    tabEnv.registerFunction("hashCode",hashCode)
    val table = tabEnv.fromDataStream(ds)
    table.select("id,hashCode(id) as hash").execute().print() // API ??????
    tabEnv.executeSql(s"select id,hashCode(id) as hash from ${table}").print() // hash??????
  }

  /**
   * UDTF ???????????????
   * +----+--------------------------------+--------------------------------+-------------+
   * | op |                           line |                           word |      length |
   * +----+--------------------------------+--------------------------------+-------------+
   * | +I |                 hello,hello,hi |                          hello |           5 |
   * | +I |                 hello,hello,hi |                          hello |           5 |
   * | +I |                 hello,hello,hi |                             hi |           2 |
   */
  @Test
  def tableFunc(): Unit ={ // UDTF
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val seq = Seq(
      "hello,hello,hi",
      "good,hello,nice",
      "nice,hello",
    )
    val ds = env.addSource(new StreamSourceMock(seq, false))
    val table = tabEnv.fromDataStream(ds,$("line"))
    val split = new Split
    tabEnv.registerFunction("split",split)

    // API ?????? UDTF
    table.joinLateral("split(line) as (word,length)").select("line,word,length").execute().print
    // SQL ?????? UDTF
    tabEnv.executeSql(s"select line,word,length from ${table},lateral table(split(line)) as splitId(word, length)").print()
  }

  /**
   * UDAF ???????????? (?????????????????????????????????????????????)
   * ???????????? ??????????????????????????????
   * getValue?????????????????????
   * createAccumulator?????????????????????
   * accumulate?????????
   * ???????????????????????????????????????????????????????????????????????????????????????????????????
   * retract ?????????????????????
   * merge ????????????
   * resetAccumulator?????????????????????
   * +----+--------------------------------+--------------------------------+
   * | op |                             id |                         avg_vc |
   * +----+--------------------------------+--------------------------------+
   * | +I |                       sensor_1 |                            1.0 |
   * | +I |                       sensor_2 |                            2.0 |
   * | -U |                       sensor_2 |                            2.0 |
   */
  @Test
  def aggregateFunction(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val avgVc = new AvgVC

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )

    val ds = env.addSource(new StreamSourceMock(seq, false))
    val table = tabEnv.fromDataStream(ds)
    tabEnv.registerFunction("avgVc",avgVc)

    // API ??????????????????
    table.groupBy("id").aggregate("avgVc(vc) as avg_vc").select("id,avg_vc").execute().print()
    // SQL ??????????????????
    tabEnv.executeSql(s"select id,avgVc(vc) as avg_vc from ${table} group by id").print()

  }

  /**
   * ????????????????????????????????????????????????????????????????????????????????????????????????????????????
   * createAccumulator ??????????????????
   * accumulate ????????????
   * retract ????????????
   * merge ????????????
   * getValue ????????????????????????
   * resetAccumulator ???????????????????????????
   * +----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
   * | op |                             id |                   window_start |                     window_end |                         avg_vc |
   * +----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
   * | +I |                       sensor_2 |            2020-12-09 15:33:10 |            2020-12-09 15:33:15 |                            2.5 |
   * | +I |                       sensor_2 |            2020-12-09 15:33:15 |            2020-12-09 15:33:20 |                         17.667 |
   * | +I |                       sensor_1 |            2020-12-09 15:33:10 |            2020-12-09 15:33:15 |                          4.333 |
   * +----+--------------------------------+--------------------------------+--------------------------------+--------------------------------+
   */
  @Test
  def windowAggregateFunction(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val windowAvgVC = new WindowAvgVC

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )

    val ds = env.addSource(new StreamSourceMock(seq, false)).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor](){
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }))
    val table = tabEnv.fromDataStream(ds,$("id"),$("ts").rowtime(),$("vc"))
    tabEnv.registerFunction("windowAvgVC",windowAvgVC)

    // API ??????????????????
    table.window(Tumble.over(lit(5).second()).on("ts").as("w"))
      .groupBy("id,w")
      .aggregate("windowAvgVC(vc) as avg_vc")
      .select("id,w.start() as window_start,w.end() as window_end,avg_vc")
      .execute()
      .print()

    // SQL ??????????????????
    tabEnv.executeSql(
      s"""select id,
         |DATE_FORMAT(TUMBLE_START(ts,INTERVAL '5' SECOND),'yyyy-MM-dd HH:mm:ss') as window_start,
         |DATE_FORMAT(TUMBLE_END(ts,INTERVAL '5' SECOND),'yyyy-MM-dd HH:mm:ss') as window_end,
         |windowAvgVC(vc) as avg_vc
         |from ${table}
         |group by id, TUMBLE(ts,INTERVAL '5' SECOND)
         |""".stripMargin)
      .print()
  }

  /**
   * UDTAGGs ???????????????
   * ???????????????????????????topN??????????????????????????????????????????UDAF??????????????????????????????????????????
   * ????????????:
   * createAccumulator
   * accumulate
   * emitValue   <<< ????????????
   * ??????????????????????????????
   * retract
   * merge
   * resetAccumulator
   * emitUpdateWithRetract << ????????????
   */
  @Test
  def tableAggregateFunction(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val top2 = new TopN
    tabEnv.registerFunction("top2",top2)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 10),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 9),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 26),
      WaterSensor("sensor_2", 1607527998000L, 11),
    )

    val ds = env.addSource(new StreamSourceMock(seq,false))
    val table = tabEnv.fromDataStream(ds)

    // ????????????SQL??????
    table.groupBy("id")
//      .flatAggregate(call("top2", $"vc").as("vcc", "rank"))
      .flatAggregate("top2(vc) as (vcc,rank)")
      .select("id,vcc,rank") // group by ?????? flatAggregate ???????????????????????????????????????
      .execute()
      .print()
  }

  /**
   * +----+--------------------------------+-------------+-------------+
| op |                             id |         vcc |        rank |
+----+--------------------------------+-------------+-------------+
| +I |                       sensor_1 |           1 |           1 |
| -D |                       sensor_1 |           1 |           1 |
| +I |                       sensor_1 |          10 |           1 |
| +I |                       sensor_1 |           1 |           2 |
| +I |                       sensor_2 |           2 |           1 |
| -D |                       sensor_2 |           2 |           1 |
| +I |                       sensor_2 |           3 |           1 |
| +I |                       sensor_2 |           2 |           2 |
| -D |                       sensor_1 |          10 |           1 |
| -D |                       sensor_1 |           1 |           2 |
| +I |                       sensor_1 |          10 |           1 |
| +I |                       sensor_1 |           9 |           2 |
| -D |                       sensor_2 |           3 |           1 |
| -D |                       sensor_2 |           2 |           2 |
| +I |                       sensor_2 |           5 |           1 |
| +I |                       sensor_2 |           3 |           2 |
| -D |                       sensor_2 |           5 |           1 |
| -D |                       sensor_2 |           3 |           2 |
| +I |                       sensor_2 |          24 |           1 |
| +I |                       sensor_2 |           5 |           2 |
| -D |                       sensor_2 |          24 |           1 |
| -D |                       sensor_2 |           5 |           2 |
| +I |                       sensor_2 |          26 |           1 |
| +I |                       sensor_2 |          24 |           2 |
| -D |                       sensor_2 |          26 |           1 |
| -D |                       sensor_2 |          24 |           2 |
| +I |                       sensor_2 |          26 |           1 |
| +I |                       sensor_2 |          24 |           2 |
+----+--------------------------------+-------------+-------------+
28 rows in set
   */
  @Test
  def retractableTableAggregateFunction(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    val top2 = new RetractableTopN
    tabEnv.registerFunction("top2",top2)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 10),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 9),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 26),
      WaterSensor("sensor_2", 1607527998000L, 11),
    )

    val ds = env.addSource(new StreamSourceMock(seq,false))
    val table = tabEnv.fromDataStream(ds)

    // ????????????SQL??????
    table.groupBy("id")
      //      .flatAggregate(call("top2", $"vc").as("vcc", "rank"))
      .flatAggregate("top2(vc) as (vcc,rank)")
      .select("id,vcc,rank") // group by ?????? flatAggregate ???????????????????????????????????????
      .execute()
      .print()
  }



}
