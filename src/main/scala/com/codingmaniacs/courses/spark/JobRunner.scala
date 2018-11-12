package com.codingmaniacs.courses.spark

import com.codingmaniacs.courses.spark.config.JobConfig
import com.codingmaniacs.courses.spark.text.TextAnalyzer
import org.apache.spark.SparkContext
import org.slf4j.{Logger, LoggerFactory}

import com.codingmaniacs.courses.spark.utils.StringUtils._

/**
  * @author ${user.name}
  */
object JobRunner {

  @transient val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {
    args match {
      case invalid if invalid == null || invalid.isEmpty => logger.warn("No arguments provided")
      case argPassed =>
        val sparkCtx = new SparkContext(JobConfig.getConfig)

        val fileName = argPassed(0)

        val wordsGrouped = TextAnalyzer.countDataPerRowInFile(fileName)(sparkCtx)
        wordsGrouped match {
          case invalid if invalid == null || invalid.isEmpty => logger.info("No data found")
          case data => data.foreach(
            w => logger.info(s"There are ${w._2._1.format()} lines starting with ${w._1}, meaning ${w._2._2.format(true)} %" )
          )
        }
    }
  }
}
