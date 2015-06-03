package com.shashank.omegapointtask

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.{rddToPairRDDFunctions, rddToOrderedRDDFunctions}


/**
 * Created by shashank on 3/6/15.
 */
object LanguageHistogram {



  def main(args: Array[String]) {
    val sc = new SparkContext(args(0),"omegapoint-task")

    val dataRDD = sc.textFile(args(1))

    val languageGroupedEntries = dataRDD.map(eachPage => {
      val fields = eachPage.split(" ")
      (fields(1),eachPage)
    }).groupByKey()

    val languageWithPagecount = languageGroupedEntries.map(eachLanguage => (eachLanguage._1,eachLanguage._2.size))

    languageWithPagecount.take(10).foreach(eachOutput => eachOutput._1+"  "+eachOutput._2)

  }

}
