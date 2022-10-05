package com.flink.study.tryflink

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.walkthrough.common.entity.Alert

class AlertSink extends SinkFunction[Alert]{

  override def invoke(value: Alert, context: SinkFunction.Context): Unit = {
    println(value)
  }

}
