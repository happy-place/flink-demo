package com.bigdata.flink.proj.dataset

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._

object WordCountApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tool = ParameterTool.fromArgs(args)
    val inputPath = tool.get("input")
    val outputPath = tool.get("output")
    env.readTextFile(inputPath)
      .flatMap(_.split("\\W+")) // \\W+ 非字母为分隔符, \\w+ 字母为分隔符
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .writeAsCsv(outputPath)
    // 必须使用 execute 才能触发
    env.execute("WordCountApp")
  }

}
