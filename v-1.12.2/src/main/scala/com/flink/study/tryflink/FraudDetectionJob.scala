package com.flink.study.tryflink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.walkthrough.common.entity.Transaction
import org.apache.flink.walkthrough.common.source.TransactionSource

/**
 * 输入端：nc -l 9000
 * 1,100
 * 1,200
 * 1,0.5
 * 1,501
 * 1,502
 * 2,0.6  <<< 输入完后等1min之后再输入下一条
 * 2,600
 *
 * 结果：只输出 Alert{id=1}
 *
 */
object FraudDetectionJob {

  def main(args: Array[String]): Unit = {
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    //    val txs = env.addSource(new TransactionSource).name("transactions")

    val txs = env.socketTextStream("localhost", 9000)
      .map { line =>
        val arr = line.split(",")
        val id = arr(0).toLong
        val amount = arr(1).toDouble
        val ts = System.currentTimeMillis() // 自行控制输入 速率

        new Transaction(id, ts, amount)
      }.name("transactions")

    val alerts = txs.keyBy(_.getAccountId).process(new FraudDetector2).name("fraud-detector")
    alerts.addSink(new AlertSink).name("send-alerts")
    env.disableOperatorChaining()
    env.execute("Fraud Detection")
  }

}
