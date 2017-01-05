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
    textAnalyzer.calculateTopNWords(sparkCtx, args(0), args(1).toInt)
  }
}
