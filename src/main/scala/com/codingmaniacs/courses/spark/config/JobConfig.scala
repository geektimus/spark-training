package com.codingmaniacs.courses.spark.config

import org.apache.spark.SparkConf

object JobConfig {
  def getConfig: SparkConf = {
    new SparkConf().setMaster("local").setAppName("SparkPractice")
  }
}
