package com.shashank.omegapointtask

import org.apache.spark.SparkContext

/**
 * Created by madhu on 20/1/15.
 */
object CountLines {

  def main(args: Array[String]) {
    val sc = new SparkContext(args(0),"omegapoint-task")

    val dataRDD = sc.textFile(args(1))

    println(dataRDD.count())
  }


}
