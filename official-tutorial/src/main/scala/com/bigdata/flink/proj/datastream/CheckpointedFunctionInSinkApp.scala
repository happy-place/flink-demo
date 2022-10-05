package com.bigdata.flink.proj.datastream

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/6 8:47 下午 
 * @desc:
 * 注：Operator State 通常在 Source 、Sink 中使用，具体跟 CheckpointedFunction 接口有关
 */
object CheckpointedFunctionInSinkApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.fromElements("hello,java","hi,java","hadoop,python")
      .flatMap(_.split(","))
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .addSink(new BufferingSink(2))

    env.execute("CheckpointedFunctionApp")
  }

  /**
   * 1.每收集 2 个元素，执行一次输出，期间元素缓存在 buffer里，完成一次输出后清除buffer 状态；
   * 2.异步触发 checkpoint 时负责将当前 临时缓存 buffer 的数据装在到 state中，且存之前需要先清除状态；
   * 3.初始化时状态后，需要检查当前是否是从 状态后端 恢复运行，如果是的话，需要将状态中是的数据载入缓存
   * @param threshold
   */
  class BufferingSink(threshold:Int = 0) extends SinkFunction[(String,Int)] with CheckpointedFunction {

    @transient private var checkpointedState:ListState[(String,Int)] = _

    private var buffers: ListBuffer[(String,Int)] = new ListBuffer[(String, Int)]

    override def invoke(value: (String, Int), context: SinkFunction.Context): Unit = {
      buffers.append(value)
      if(buffers.size == threshold){
        buffers.foreach(println(_))
        buffers.clear()
        // TODO 使用休眠，显示两个元素一次输出效果
        TimeUnit.SECONDS.sleep(2)
      }
    }

    override def snapshotState(ctx: FunctionSnapshotContext): Unit = {
      checkpointedState.clear()
      for(elem <- buffers){
        checkpointedState.add(elem)
      }
    }

    override def initializeState(ctx: FunctionInitializationContext): Unit = {
      val listStateDesc = new ListStateDescriptor("checkpoint-state",TypeInformation.of(new TypeHint[(String,Int)](){}))
      checkpointedState = ctx.getOperatorStateStore.getListState(listStateDesc)
      if(ctx.isRestored){
        checkpointedState.get().forEach(elem =>buffers.append(elem))
      }
    }
  }

}


