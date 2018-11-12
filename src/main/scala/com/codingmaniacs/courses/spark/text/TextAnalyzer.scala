package com.codingmaniacs.courses.spark.text

import org.apache.spark.SparkContext
import org.slf4j.{Logger, LoggerFactory}

object TextAnalyzer {
  @transient val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * Calculate and display the top N most common words from a file
    *
    * @param fileName         Name of the file to be processed.
    * @param numberOfElements Number of words to be retrieved
    * @param sparkContext     Spark context required to run the computation
    */
  def calculateTopNWordsInFile(fileName: String, numberOfElements: Int = 10)(implicit sparkContext: SparkContext): Unit = {
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
    * Calculate and display the top N most common words from a file
    *
    * @param data         Name of the file to be processed.
    * @param numberOfElements Number of words to be retrieved
    * @param sparkContext     Spark context required to run the computation
    */
  def calculateTopNWordsInText(data: String, numberOfElements: Int = 10)(implicit sparkContext: SparkContext): Array[(String, Int)] = {
    if (sparkContext == null || data == null || data.trim.isEmpty) {
      logger.error("No enough parameters provided to perform the analysis.")
      return Array[(String, Int)]()
    }

    val text = sparkContext.parallelize(data.split("\n"))

    val analytics =
      text
        .flatMap(lines => lines.split(" "))
        .map(word => word.toLowerCase.replaceAll("[,.]", ""))
        .filter(word => !word.trim.isEmpty && word.trim.length > 3)
        .map(word => (word, 1))
        .reduceByKey((accumulator, n) => accumulator + n)
        .sortBy(value => value._2, ascending = false)

    analytics.collect().take(numberOfElements)
  }

  /**
    * Calculate the number of words in a file.
    *
    * @param sparkContext Spark context required to run the computation
    * @param fileName     Name of the file to be processed.
    * @return
    */
  def wordCountInFile(fileName: String)(implicit sparkContext: SparkContext): Int = {
    if (sparkContext == null || fileName == null || fileName.trim.isEmpty) {
      logger.error("No enough parameters provided to perform the analysis.")
      return -1
    }
    val text = sparkContext.textFile(fileName)
    text
      .flatMap(lines => lines.split(" "))
      .map(word => word.toLowerCase.replaceAll("[,.]", ""))
      .map(_ => 1)
      .reduce(_ + _)
  }

  /**
    * Calculate the number of words in a file.
    *
    * @param sparkContext Spark context required to run the computation
    * @param data         Name of the file to be processed.
    * @return
    */
  def wordCountInText(data: String)(implicit sparkContext: SparkContext): Int = {
    if (sparkContext == null || data == null || data.trim.isEmpty) {
      logger.error("No enough parameters provided to perform the analysis.")
      return -1
    }
    val text = sparkContext.parallelize(data.split("\n"))

    text
      .flatMap(lines => lines.split(" "))
      .map(word => word.toLowerCase.replaceAll("[,.]", ""))
      .map(_ => 1)
      .reduce(_ + _)
  }

  /**
    * Calculate how many lines start with the same word and group them.
    *
    * @param fileName     Name of the file to be processed.
    * @param sparkContext Spark context required to run the computation
    * @return
    */
  def countDataPerRowInFile(fileName: String)(implicit sparkContext: SparkContext): Array[(String, (Long, Double))] = (sparkContext, fileName) match {
    case (ctx, _) if ctx == null =>
      logger.error("The required spark context to perform this operation was not provided.")
      Array[(String, (Long, Double))]()

    case (_, name) if name == null || name.trim.isEmpty =>
      logger.error("Cannot perform analysis without a source file.")
      Array[(String, (Long, Double))]()

    case (ctx, name) =>
      val text = ctx.textFile(name)
      val counts = text
        .map(line => line.split(" ")(0))
        .map(startWord => (startWord, 1L))
        .aggregateByKey(0L)((sum, pair) => sum + pair, _ + _)

      val totals = counts.map(_._2).reduce((x, y) => x + y)

      val percentageTuple = (total: Long) => (number: Long) => {
        (number, number * 100.0 / total)
      }

      val calculatePercentage = percentageTuple(totals)

      counts
        .mapValues(c => calculatePercentage(c))
        .sortBy(a => a._2._2, ascending = false)
        .collect()
  }

  /**
    * Calculate how many lines start with the same word and group them.
    *
    * @param contents     Data to be processed.
    * @param sparkContext Spark context required to run the computation
    * @return
    */
  def countDataPerRowInText(contents: String)(implicit sparkContext: SparkContext): Array[(String, (Long, Double))] = (sparkContext, contents) match {
    case (ctx, _) if ctx == null =>
      logger.error("The required spark context to perform this operation was not provided.")
      Array[(String, (Long, Double))]()

    case (_, data) if data == null || data.trim.isEmpty =>
      logger.error("Cannot perform analysis without data.")
      Array[(String, (Long, Double))]()

    case (ctx, data) =>
      val text = ctx.parallelize[String](data.split("\n"))

      val counts = text
        .map(line => line.split(" ")(0))
        .map(startWord => (startWord, 1L))
        .aggregateByKey(0L)((sum, pair) => sum + pair, _ + _)

      val totals = counts.map(_._2).reduce((x, y) => x + y)

      val percentageTuple = (total: Long) => (number: Long) => {
        (number, number * 100.0 / total)
      }

      val calculatePercentage = percentageTuple(totals)

      counts
        .mapValues(c => calculatePercentage(c))
        .sortBy(a => a._2._2, ascending = false)
        .collect()
  }
}
