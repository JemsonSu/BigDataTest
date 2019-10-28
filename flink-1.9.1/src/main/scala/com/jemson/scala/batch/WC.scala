package com.jemson.scala.batch

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
// flatMap 不导入会报错
import org.apache.flink.api.scala._

/**
 * flink scala版 wordcount
 */
object WC {
  def main(args: Array[String]): Unit = {
    //封装所以参数，方便后面直接用get取出来
    val params: ParameterTool = ParameterTool.fromArgs(args)

    //准备环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //当前输入文件
    //val inputFile: String = params.get("inputFile")
    val inputFile: String = "./files/wc.txt"
    val text: DataSet[String] = env.readTextFile(inputFile)

    //压平
    val ds: DataSet[String] = text.flatMap(line => line.split(" "))
    //映射
    val ds2: DataSet[(String, Int)] = ds.map(word => (word,1))
    //分组  指定tuple2的第一个字段分组
    val gds: GroupedDataSet[(String, Int)] = ds2.groupBy(0)
    //叠加1
    val ads: AggregateDataSet[(String, Int)] = gds.sum(1)
    //打印
    ads.print()





  }
}
