package com.bigdata.flink.proj.datastream

import org.apache.flink.api.common.state.{CheckpointListener, ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/6 9:12 下午 
 * @desc:
 *
 * bin/flink run -s savepoint 演示从指定 保存点恢复
 *
 */
object CheckpointedFunctionInSourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(5000) // 间隔 5 秒执行一次 checkpoint
    val path = getClass.getClassLoader.getResource("backend").getPath.split("target")(0)
    env.setStateBackend(new FsStateBackend(s"file:///${path}/src/main/resources/backend"))

    env.addSource(new CounterSource)
      .print()
    env.execute("CheckpointedFunctionInSourceApp")
  }

  /**
   * 1.自定义输入源，为维持 exactly-once 语义，需要实现 CheckpointedFunction接口。且只需 checkpoint 时需要上锁。
   *
   */
  class CounterSource extends RichParallelSourceFunction[Long] with CheckpointedFunction with CheckpointListener{

    @volatile private var isRunning = true
    // TODO ListState 过长，是否考虑只保留最近 5 条记录？
    private var state:ListState[Long] = _
    private var offset = 0L

    override def snapshotState(ctx: FunctionSnapshotContext): Unit = {
      state.clear()
      state.add(offset)
    }

    override def initializeState(ctx: FunctionInitializationContext): Unit = {
      state = ctx.getOperatorStateStore.getUnionListState(new ListStateDescriptor("offset-state",classOf[Long]))
      for(o <- state.get().asScala){
        offset = o
      }
    }

    override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
      val lock = ctx.getCheckpointLock
      while(isRunning){
        lock.synchronized{
          ctx.collect(offset)
          offset += 1
        }
        TimeUnit.SECONDS.sleep(1)
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }

    override def notifyCheckpointComplete(checkpointId: Long): Unit = {
      println(s"checkpoint id: ${checkpointId} success")
    }
  }

}

