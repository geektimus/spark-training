package com.codingmaniacs.courses.spark.generic

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * This class contains all the tests related to single RDD over Single RDD Transformations.
  */
class SimpleRDDTransformationsTest extends FunSuite with SharedSparkContext {

  test("basic map transformation") {
    val values = List(1, 2, 3, 4, 5)
    val valRDD: RDD[Int] = sc.parallelize(values)

    val expected = Array(2, 3, 4, 5, 6)
    val res = valRDD.map(x => x + 1).collect()
    assert(res.sameElements(expected))
  }

  test("basic flatMap transformation") {
    val values = List("spark apache scala", "java operations tests")
    val valRDD = sc.parallelize(values)

    val expected = Array("spark", "apache", "scala", "java", "operations", "tests")
    val res = valRDD.flatMap(phrase => phrase.split(" ")).collect()
    assert(res.sameElements(expected))
  }

  test("basic filter transformation") {
    val values = List(1, 2, 3, 4, 5)
    val valRDD: RDD[Int] = sc.parallelize(values)

    val expected = Array(1, 5)
    val res = valRDD.filter(x => x < 2 || x > 4).collect()

    assert(res.sameElements(expected))
  }

  test("basic distinct transformation") {
    val values = "remember"
    val valRDD = sc.parallelize(values.split(""))

    val expected = "embr"
    val res = valRDD.distinct().collect().mkString("")

    assert(res.equals(expected))
  }

  test("basic sample transformation") {
    val values = "remember"
    val valRDD = sc.parallelize(values.split(""))
    val expected = values.split("")

    val res = valRDD.sample(withReplacement = false, 1).collect()
    assert(res.sameElements(expected))
  }
}
