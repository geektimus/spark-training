package com.codingmaniacs.courses.spark.generic

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

class MultipleRDDTransformationsTest extends FunSuite with SharedSparkContext with BeforeAndAfter {

  @transient val logger: Logger = LoggerFactory.getLogger(classOf[MultipleRDDTransformationsTest])

  val firstList = List(1, 2, 3, 4)
  val secondList = List(4, 5, 6, 7)

  var firstRDD: RDD[Int] = _
  var secondRDD: RDD[Int] = _

  before {
    firstRDD = sc.parallelize(firstList)
    secondRDD = sc.parallelize(secondList)
  }

  test("basic union transformation") {
    logger.debug("basic union transformation")

    val expected = Array(1, 2, 3, 4, 4, 5, 6, 7)
    val res = firstRDD.union(secondRDD).collect()
    assert(res.sameElements(expected))
  }

  test("basic intersection transformation") {
    logger.debug("basic intersection transformation")

    val expected = Array(4)
    val res = firstRDD.intersection(secondRDD).collect()
    assert(res.sameElements(expected))
  }

  test("basic subtract transformation (A - B)") {
    logger.debug("basic subtract transformation (A - B)")

    val expected = Array(1, 2, 3)
    val res = firstRDD.subtract(secondRDD).collect()
    assert(res.sameElements(expected))
  }

  test("basic subtract transformation (B - A)") {
    logger.debug("basic subtract transformation (B - A)")

    val expected = Array(5, 6, 7)
    val res = secondRDD.subtract(firstRDD).collect()
    assert(res.sameElements(expected))
  }

  test("basic cartesian transformation (A * B)") {
    logger.debug("basic cartesian transformation (A * B)")

    val expected = Array(
      (1, 4), (1, 5), (1, 6), (1, 7),
      (2, 4), (2, 5), (2, 6), (2, 7),
      (3, 4), (3, 5), (3, 6), (3, 7),
      (4, 4), (4, 5), (4, 6), (4, 7))

    val res = firstRDD.cartesian(secondRDD).collect()
    assert(res.sameElements(expected))
  }

  test("basic cartesian transformation (B * A)") {
    logger.debug("basic cartesian transformation (B * A)")

    val expected = Array(
      (4, 1), (4, 2), (4, 3), (4, 4),
      (5, 1), (5, 2), (5, 3), (5, 4),
      (6, 1), (6, 2), (6, 3), (6, 4),
      (7, 1), (7, 2), (7, 3), (7, 4))

    val res = secondRDD.cartesian(firstRDD).collect()
    assert(res.sameElements(expected))
  }
}
