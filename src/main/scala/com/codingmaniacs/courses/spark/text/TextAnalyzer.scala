package com.codingmaniacs.courses.spark.text

import org.apache.spark.SparkContext
import org.slf4j.{Logger, LoggerFactory}

class TextAnalyzer {
  @transient val logger: Logger = LoggerFactory.getLogger(classOf[TextAnalyzer])

  /**
    * Calculate and display the top N most common words from a file
    *
    * @param sparkContext     Spark context required to run the computation
    * @param fileName         Name of the file to be processed.
    * @param numberOfElements Number of words to be retrieved
    */
  def calculateTopNWords(sparkContext: SparkContext, fileName: String, numberOfElements: Int = 10): Unit = {
    if (sparkContext == null || fileName == null || fileName.trim.isEmpty) {
      logger.error("No enough parameters provided to perform the analysis.")
      return
    }

    val text = sparkContext.textFile(fileName)
    val analytics =
      text
        .flatMap(lines => lines.split(" "))
        .map(word => word.toLowerCase.replaceAll("[,.]", ""))
        .filter(word => !word.trim.isEmpty && word.trim.length > 3)
        .map(word => (word, 1))
        .reduceByKey((accumulator, n) => accumulator + n)
        .sortBy(value => value._2, ascending = false)

    val topN = analytics.collect().take(numberOfElements)

    topN.foreach(row => logger.debug("Word: [{}] - Count: [{}]", row._1, row._2))

  }

  /**
    * Calculate the number of words in a file.
    *
    * @param sparkContext Spark context required to run the computation
    * @param fileName     Name of the file to be processed.
    * @return
    */
  def wordCount(sparkContext: SparkContext, fileName: String): Int = {
    if (sparkContext == null || fileName == null || fileName.trim.isEmpty) {
      logger.error("No enough parameters provided to perform the analysis.")
      return -1
    }
    val text = sparkContext.textFile(fileName)
    val analytics =
      text
        .flatMap(lines => lines.split(" "))
        .map(word => word.toLowerCase.replaceAll("[,.]", ""))
        .map(word => 1)
        .reduce(_ + _)


    analytics.toInt
  }
}
