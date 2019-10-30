package com.jemson.scala.stream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * scala版 flink   SocketWindowWC
 */
object SocketWindowWC {

  def main(args: Array[String]): Unit = {
    val parames: ParameterTool = ParameterTool.fromArgs(args)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    val hostname = parames.get("hostname")
    val port = parames.getInt("port")
    val lines: DataStream[String] = env.socketTextStream(hostname, port)

    lines.flatMap(line => line.split("\\s"))  //切割字符或单词，空格、制表符
      .map(word => (word,1)) //映射
      .keyBy(0)  //第一个字段进行分组
      .timeWindow(Time.seconds(3), Time.seconds(2)) //设置窗口3s和间隔2s
      .sum(1) //把第二个字段进行累加
      .print() //打印

    env.execute("SocketWindowWC") //执行




  }
}
