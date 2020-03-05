/*
 * Copyright (c) 2020 Geektimus
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.codingmaniacs.courses.spark.text

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.{ Logger, LoggerFactory }

import scala.util.matching.Regex

object TextAnalyzer {

  private val percentageTuple = (total: Long) =>
    (number: Long) => {
      (number, number * 100.0 / total)
    }

  @transient val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
    * Calculate and display the top N most common words from a file
    *
    * @param fileName         Name of the file to be processed.
    * @param numberOfElements Number of words to be retrieved
    * @param sparkContext     Spark context required to run the computation
    */
  def calculateTopNWordsInFile(fileName: String, numberOfElements: Int = 10)(
    implicit sparkContext: SparkContext
  ): Array[(String, Int)] = {
    if (sparkContext == null || fileName == null || fileName.trim.isEmpty) {
      logger.error("No enough parameters provided to perform the analysis.")
      return Array[(String, Int)]()
    }

    calculateTopNWordsInText(sparkContext.textFile(fileName), numberOfElements)
  }

  /**
    * Calculate and display the top N most common words from a file
    *
    * @param data             Name of the file to be processed.
    * @param numberOfElements Number of words to be retrieved
    */
  def calculateTopNWordsInText(
    data: RDD[String],
    numberOfElements: Int = 10
  ): Array[(String, Int)] = {

    val analytics =
      data
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
    wordCountInText(sparkContext.textFile(fileName))
  }

  /**
    * Calculate the number of words in a file.
    *
    * @param data Name of the file to be processed.
    * @return
    */
  def wordCountInText(data: RDD[String]): Int =
    data
      .flatMap(lines => lines.split(" "))
      .map(word => word.toLowerCase.replaceAll("[,.]", ""))
      .map(_ => 1)
      .reduce(_ + _)

  /**
    * Calculate how many lines start with the same word and group them.
    *
    * @param fileName     Name of the file to be processed.
    * @param sparkContext Spark context required to run the computation
    * @return
    */
  def countDataPerRowInFile(regex: Regex, fileName: String)(
    implicit sparkContext: SparkContext
  ): Array[(String, (Long, Double))] = fileName match {
    case name if name == null || name.trim.isEmpty =>
      logger.error("Cannot perform analysis without a source file.")
      Array[(String, (Long, Double))]()

    case name => countDataPerRowInText(regex, sparkContext.textFile(name))
  }

  /**
    * Calculate how many lines start with the same word and group them.
    *
    * @param contents Data to be processed.
    * @return
    */
  def countDataPerRowInText(regex: Regex, contents: RDD[String]): Array[(String, (Long, Double))] =
    contents match {

      case data if data == null || data.isEmpty =>
        logger.error("Cannot perform analysis without data.")
        Array[(String, (Long, Double))]()

      case data =>
        val counts =
          data
            .map { line =>
              val regex(value) = line
              value
            }
            .map(startWord => (startWord, 1L))
            .aggregateByKey(0L)((sum, pair) => sum + pair, _ + _)

        val totals = counts.map(_._2).reduce((x, y) => x + y)

        val calculatePercentage = percentageTuple(totals)

        counts
          .mapValues(c => calculatePercentage(c))
          .sortBy(a => a._2._2, ascending = false)
          .collect()
    }
}
