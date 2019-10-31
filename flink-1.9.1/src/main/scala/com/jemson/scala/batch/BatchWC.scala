package com.jemson.scala.batch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * flink scala 版 WC  批处理
 */
object BatchWC {

  def main(args: Array[String]): Unit = {
    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)

    val inputFile: String = parameterTool.get("inputFile")

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val lines: DataSet[String] = env.readTextFile(inputFile)

    //打印文本内容
    //lines.print()

    val endDS: AggregateDataSet[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))
      .groupBy(0)
      .sum(1)

    endDS.print()




  }
}
