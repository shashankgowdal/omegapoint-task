package com.shashank.omegapointtask

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.{rddToPairRDDFunctions, rddToOrderedRDDFunctions}


/**
 * Created by shashank on 3/6/15.
 */
object LanguageHistogram {

  def main(args: Array[String]) {
    //Create spark context
    val sc = new SparkContext(args(0),"omegapoint-task")

    //Load the text file and create an RDD
    val dataRDD = sc.textFile(args(1))

    //Parse each line of the file, extract the language field and group with respect to it.
    val languageGroupedEntries = dataRDD.map(eachPage => {
      val fields = eachPage.split(" ")
      (fields(1),eachPage)
    }).groupByKey()

    //Find out the number of entries for each language by taking the size of the grouped collection wrt each language
    val languageWithPagecount = languageGroupedEntries.map(eachLanguage => (eachLanguage._1,eachLanguage._2.size))

    //Take 10 languages and its page count, prints the values
    languageWithPagecount.take(10).foreach(eachOutput => eachOutput._1+"  "+eachOutput._2)
  }

}
