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

package com.codingmaniacs.courses.spark.singlerdds

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{ BeforeAndAfter, FunSuite }

/**
  * This class contains all the tests related to Transformations over two RDDs.
  */
class MultipleRDDTransformationsTest extends FunSuite with SharedSparkContext with BeforeAndAfter {

  val firstList = List(1, 2, 3, 4)
  val secondList = List(4, 5, 6, 7)

  var firstRDD: RDD[Int] = _
  var secondRDD: RDD[Int] = _

  before {
    firstRDD = sc.parallelize(firstList)
    secondRDD = sc.parallelize(secondList)
  }

  test("basic union transformation") {
    val expected = Array(1, 2, 3, 4, 4, 5, 6, 7)
    val res = firstRDD.union(secondRDD).collect()
    assert(res.sameElements(expected))
  }

  test("basic intersection transformation") {
    val expected = Array(4)
    val res = firstRDD.intersection(secondRDD).collect()
    assert(res.sameElements(expected))
  }

  test("basic subtract transformation (A - B)") {
    val expected = Array(1, 2, 3)
    val res = firstRDD.subtract(secondRDD).sortBy(x => x).collect()
    assert(res.sameElements(expected))
  }

  test("basic subtract transformation (B - A)") {
    val expected = Array(5, 6, 7)
    val res = secondRDD.subtract(firstRDD).sortBy(x => x).collect()
    assert(res.sameElements(expected))
  }

  test("basic cartesian transformation (A * B)") {
    val expected = Array(
      (1, 4),
      (1, 5),
      (1, 6),
      (1, 7),
      (2, 4),
      (2, 5),
      (2, 6),
      (2, 7),
      (3, 4),
      (3, 5),
      (3, 6),
      (3, 7),
      (4, 4),
      (4, 5),
      (4, 6),
      (4, 7)
    )

    val res = firstRDD.cartesian(secondRDD).sortBy(x => x).collect()
    assert(res.sameElements(expected))
  }

  test("basic cartesian transformation (B * A)") {
    val expected = Array(
      (4, 1),
      (4, 2),
      (4, 3),
      (4, 4),
      (5, 1),
      (5, 2),
      (5, 3),
      (5, 4),
      (6, 1),
      (6, 2),
      (6, 3),
      (6, 4),
      (7, 1),
      (7, 2),
      (7, 3),
      (7, 4)
    )

    val res = secondRDD.cartesian(firstRDD).sortBy(x => x).collect()
    assert(res.sameElements(expected))
  }
}
