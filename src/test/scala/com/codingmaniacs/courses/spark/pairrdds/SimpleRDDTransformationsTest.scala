package com.codingmaniacs.courses.spark.pairrdds

import java.util.Locale

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.{Logger, LoggerFactory}

/**
  * This class contains all the tests related to single RDD over Single RDD Transformations.
  */
class SimpleRDDTransformationsTest extends FunSuite with SharedSparkContext {

  @transient
  lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val extract: String =
    """Bilbo drew his hand over his eyes. I am sorry, he said. But I felt so
      queer. And yet it would be a relief in a way not to be bothered with it any
      more. It has been so growing on my mind lately. Sometimes I have felt it was
      like an eye looking at me. And I am always wanting to put it on and
      disappear, don't you know; or wondering if it is safe, and pulling it out to
      make sure. I tried locking it up, but I found I couldn't rest without it in
      my pocket. I don't know why. And I don't seem able to make up my mind.
    """

  test("basic reduceByKey transformation") {
    val extractRDD = sc.parallelize(extract.split("\n"))

    val reduceBy = extractRDD
      .flatMap(lines => lines.toLowerCase(Locale.ENGLISH).split(" "))
      .map(word => word.replaceAll("[;.,]", ""))
      .filter(x => x.length > 8)
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)

    val result = reduceBy.collect()

    val expected = Array(("sometimes", 1), ("disappear", 1), ("wondering", 1))

    assert(result.sameElements(expected))
  }

  test("basic groupByKey transformation") {
    val extractRDD = sc.parallelize(extract.split("\n"))

    val groupByKey = extractRDD
      .flatMap(lines => lines.toLowerCase(Locale.ENGLISH).split(" "))
      .map(word => word.replaceAll("[;.,]", ""))
      .map(word => (word, 1))
      .groupByKey()
      .

    groupByKey.foreach(x => x._2.foreach(println))
  }
}
