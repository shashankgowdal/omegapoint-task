package com.shashank.omegapointtask

import org.apache.spark.SparkContext

/**
 * Created by madhu on 20/1/15.
 */
object CountLines {

  def main(args: Array[String]) {
    //Create spark context
    val sc = new SparkContext(args(0),"omegapoint-task")

    //Load the text file and create an RDD
    val dataRDD = sc.textFile(args(1))

    //Count the number of lines in RDD
    println(dataRDD.count())
  }

}
