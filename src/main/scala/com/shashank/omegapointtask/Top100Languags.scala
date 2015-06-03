package com.shashank.omegapointtask

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.{numericRDDToDoubleRDDFunctions, rddToOrderedRDDFunctions, rddToPairRDDFunctions}

/**
 * Created by shashank on 3/6/15.
 */
object Top100Languags {

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

    //Sort the languages in descending order of their page count.
    val languageWithPagecountSorted = languageWithPagecount.map(_.swap).sortByKey(false).map(_.swap)

    //Take top 100 languages based on their page count
    languageWithPagecountSorted.take(100).foreach(eachOutput => println(eachOutput._1+"  "+eachOutput._2))
  }

}
