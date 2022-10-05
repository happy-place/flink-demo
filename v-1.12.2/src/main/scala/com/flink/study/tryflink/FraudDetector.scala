package com.flink.study.tryflink

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.{Alert, Transaction}

object FraudDetector{
  val SMALL_AMOUNT:Double = 1.00
  val LARGE_AMOUNT:Double = 500.00
  val ONE_MINUTE:Long = 60*1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long,Transaction,Alert]{

  @transient private var flagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)
    val timerDescriptor = new ValueStateDescriptor("timer", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)
  }

  override def processElement(transaction: Transaction, context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
                              collector: Collector[Alert]): Unit = {
    // Get the current state for the current key
    val lastTransactionWasSmall = flagState.value

    // Check if the flag is set
    if (lastTransactionWasSmall != null) {
      if (transaction.getAmount > FraudDetector.LARGE_AMOUNT) {
        // Output an alert downstream
        val alert = new Alert
        alert.setId(transaction.getAccountId)

        collector.collect(alert)
      }
      // Clean up our state
      cleanUp(context)
    }

    if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
      // set the flag to true
      flagState.update(true)
      val expireAt = context.timerService().currentProcessingTime() + FraudDetector.ONE_MINUTE
      context.timerService().registerProcessingTimeTimer(expireAt)
      timerState.update(expireAt)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
                       out: Collector[Alert]): Unit = {
    flagState.clear()
    timerState.clear()
  }

  @throws[Exception]
  private def cleanUp(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
    // delete timer
    val timer = timerState.value
    ctx.timerService.deleteProcessingTimeTimer(timer)

    // clean up all states
    timerState.clear()
    flagState.clear()
  }

}


