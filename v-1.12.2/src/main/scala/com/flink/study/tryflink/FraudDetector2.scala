package com.flink.study.tryflink

import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.{Alert, Transaction}


@SerialVersionUID(1L)
class FraudDetector2 extends KeyedProcessFunction[Long,Transaction,Alert]{

  @transient private var flagState: ValueState[java.lang.Boolean] = _

  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    val ttl = StateTtlConfig.newBuilder(Time.milliseconds(FraudDetector.ONE_MINUTE))
      .updateTtlOnCreateAndWrite()
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired) // 一旦过去无法访问
      .useProcessingTime() // 在默认eventTime 语义下，需要显示声明使用processTime
      .build()

    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagDescriptor.enableTimeToLive(ttl)
    flagState = getRuntimeContext.getState(flagDescriptor)
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
      flagState.clear()
    }

    if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
      // set the flag to true
      flagState.update(true)
    }
  }

}


