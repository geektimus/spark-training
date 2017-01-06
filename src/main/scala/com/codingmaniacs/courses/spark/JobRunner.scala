package com.codingmaniacs.courses.spark

import com.codingmaniacs.courses.spark.config.JobConfig
import com.codingmaniacs.courses.spark.text.TextAnalyzer
import org.apache.spark.SparkContext
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author ${user.name}
  */
object JobRunner {

  @transient val logger: Logger = LoggerFactory.getLogger("com.codingmaniacs.courses.spark.JobRunner")

  def main(args: Array[String]) {

    val sparkCtx = new SparkContext(JobConfig.getConfig)

    val textAnalyzer = new TextAnalyzer()
    val fileName = args(0)
    logger.debug("The file {} contains {} words", fileName, textAnalyzer.wordCount(sparkCtx, fileName))

  }
}
