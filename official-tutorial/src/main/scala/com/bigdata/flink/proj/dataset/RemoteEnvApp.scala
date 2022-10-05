package com.bigdata.flink.proj.dataset

import org.apache.flink.api.scala._

object RemoteEnvApp {

  def main(args: Array[String]): Unit = {
    val jarFiles = "/Users/huhao/softwares/idea_proj/flink-demo/official-tutorial/target/official-tutorial-jar-with-dependencies.jar"
    // job manager dispatcher 的 web ui 地址
    val env = ExecutionEnvironment.createRemoteEnvironment("hadoop01",8081,jarFiles)
    env.fromElements("hello,python","scala,hello","python,hello")
      .flatMap(_.split(","))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .print()
    env.execute()
  }

}
