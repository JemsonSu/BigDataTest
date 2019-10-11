package com.jemson

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaSparkWC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("ScalaSparkWC")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("./files/wc.txt")
    val works: RDD[String] = lines.flatMap(line => line.split(" "))
    val ones: RDD[(String, Int)] = works.map(work => (work,1))
    val counts: RDD[(String, Int)] = ones.reduceByKey((v1,v2) => v1+v2)
    counts.foreach(println)

    println(counts.count())

  }
}
