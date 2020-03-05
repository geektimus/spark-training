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
import org.scalatest.FunSuite

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

    val expected = "bemr"
    val res = valRDD.distinct().sortBy(x => x).collect().mkString("")
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
