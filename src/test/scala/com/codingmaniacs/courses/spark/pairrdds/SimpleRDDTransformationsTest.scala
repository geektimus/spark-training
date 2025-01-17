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

package com.codingmaniacs.courses.spark.pairrdds

import java.util.Locale
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** This class contains all the tests related to single RDD over Single RDD
  * Transformations.
  */
class SimpleRDDTransformationsTest
    extends AnyFunSuite
    with SharedSparkContext
    with Matchers {

  val extract: String =
    """
      Bilbo drew his hand over his eyes. I am sorry, he said. But I felt so
      queer. And yet it would be a relief in a way not to be bothered with it any
      more. It has been so growing on my mind lately. Sometimes I have felt it was
      like an eye looking at me. And I am always wanting to put it on and
      disappear, don't you know; or wondering if it is safe, and pulling it out to
      make sure. I tried locking it up, but I found I couldn't rest without it in
      my pocket. I don't know why. And I don't seem able to make up my mind.
    """

  test("basic reduceByKey transformation") {
    val extractRDD = sc.parallelize(extract.split("\n").toIndexedSeq)

    val reduceBy = extractRDD
      .flatMap(lines =>
        lines.toLowerCase(Locale.ENGLISH).split(" ").toIndexedSeq
      )
      .map(word => word.replaceAll("[;.,\n\r]", ""))
      .filter(x => x.length > 8)
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)
      .sortByKey(ascending = true)

    val result = reduceBy.collect()

    val expected = Array(("disappear", 1), ("sometimes", 1), ("wondering", 1))

    assert(result.sameElements(expected))
  }

  test("basic groupByKey transformation") {
    val extractRDD = sc.parallelize(extract.split("\n").toIndexedSeq)

    val groupByKey = extractRDD
      .flatMap(lines =>
        lines.toLowerCase(Locale.ENGLISH).split(" ").toIndexedSeq
      )
      .map(word => word.replaceAll("[;.,\n\r]", ""))
      .filter(word => word.length > 1)
      .map(word => (word, 1))
      .groupByKey()
      .map(x => (x._1, x._2.size))
      .filter(x => x._2 > 3)
      .collectAsMap()

    val expected = Map("and" -> 5, "to" -> 4, "it" -> 9)

    assert(groupByKey == expected)
  }

  test("basic combineByKey transformation") {
    type ScoreCollector = (Int, Double)
    type PersonScores = (String, (Int, Double))

    val Eps = 1e-3

    val initialScores = Array(
      ("Fred", 88.0),
      ("Fred", 95.0),
      ("Fred", 91.0),
      ("Wilma", 93.0),
      ("Wilma", 95.0),
      ("Wilma", 98.0)
    )

    val wilmaAndFredScores = sc.parallelize(initialScores.toIndexedSeq).cache()

    val createScoreCombiner = (score: Double) => (1, score)

    val scoreCombiner = (collector: ScoreCollector, score: Double) => {
      val (numberScores, totalScore) = collector
      (numberScores + 1, totalScore + score)
    }

    val scoreMerger =
      (collector1: ScoreCollector, collector2: ScoreCollector) => {
        val (numScores1, totalScore1) = collector1
        val (numScores2, totalScore2) = collector2
        (numScores1 + numScores2, totalScore1 + totalScore2)
      }
    val scores = wilmaAndFredScores.combineByKey(
      createScoreCombiner,
      scoreCombiner,
      scoreMerger
    )

    val averagingFunction = (personScore: PersonScores) => {
      val (name, (numberScores, totalScore)) = personScore
      (name, totalScore / numberScores)
    }

    val averageScores = scores.collectAsMap().map(averagingFunction)

    averageScores.getOrElse("Fred", 0.0) should be(91.333 +- Eps)
    averageScores.getOrElse("Wilma", 0.0) should be(95.333 +- Eps)
  }

  test("basic mapValues transformation") {
    val extractRDD = sc.parallelize(extract.split("\n").toIndexedSeq)

    val wordCounts = extractRDD
      .flatMap(lines =>
        lines.toLowerCase(Locale.ENGLISH).split(" ").toIndexedSeq
      )
      .map(word => word.replaceAll("[;.,\n\r]", ""))
      .filter(x => x.nonEmpty)
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)
      .persist()

    val totalWordCount = wordCounts
      .map(_._2)
      .reduce((x, y) => x + y)

    val wordWeights = wordCounts
      .mapValues(value => math round ((value * 100) / totalWordCount.toFloat))
      .filter(value =>
        value._2 > 5
      ) // Filter words with less than 5% of occurrences.
      .collectAsMap()

    val expectedPercentage = Map("it" -> 8, "i" -> 8)

    assert(wordWeights.equals(expectedPercentage))

  }

  test("basic flatMapValues transformation") {
    val extractRDD = sc.parallelize(extract.split("\n").toIndexedSeq)

    val wordsByInitialLetter = extractRDD
      .flatMap(lines =>
        lines.toLowerCase(Locale.ENGLISH).split(" ").toIndexedSeq
      )
      .map(word => word.replaceAll("[;.,\n\r]", ""))
      .filter(word => word.nonEmpty)
      .map(word => (word.charAt(0), 1))
      .groupByKey()

    val wordsCount = wordsByInitialLetter
      .flatMapValues(x => x)
      .values
      .reduce((x, y) => x + y)

    val expectedWordCount = 113

    assert(wordsCount == expectedWordCount)
  }

  test("basic keys retrieval") {
    val extractRDD = sc.parallelize(extract.split("\n").toIndexedSeq)

    val wordsByInitialLetter = extractRDD
      .flatMap(lines =>
        lines.toLowerCase(Locale.ENGLISH).split(" ").toIndexedSeq
      )
      .map(word => word.replaceAll("[;.,\n\r]", ""))
      .filter(word => word.nonEmpty)
      .map(word => (word.charAt(0), 1))
      .groupByKey()

    val keys = wordsByInitialLetter.keys.collect().sortBy(x => x).mkString("")

    val expectedResult = "abcdefghiklmnopqrstuwy"
    assert(keys == expectedResult)

  }

  test("basic values retrieval") {
    val extractRDD = sc.parallelize(extract.split("\n").toIndexedSeq)

    val wordsByInitialLetter = extractRDD
      .flatMap(lines =>
        lines.toLowerCase(Locale.ENGLISH).split(" ").toIndexedSeq
      )
      .map(word => word.replaceAll("[;.,\n\r]", ""))
      .filter(word => word.nonEmpty)
      .map(word => (word.charAt(0), 1))
      .reduceByKey((x, y) => x + y)

    val values = wordsByInitialLetter.values.reduce((x, y) => x + y)

    val expectedResult = 113
    assert(values == expectedResult)

  }

  test("basic sortByKeys transformation") {
    val extractRDD = sc.parallelize(extract.split("\n").toIndexedSeq)

    val wordsByInitialLetter = extractRDD
      .flatMap(lines =>
        lines.toLowerCase(Locale.ENGLISH).split(" ").toIndexedSeq
      )
      .map(word => word.replaceAll("[;.,\n\r]", ""))
      .filter(word => word.nonEmpty)
      .map(word => (word.charAt(0), 1))
      .reduceByKey((x, y) => x + y)
      .sortByKey(ascending = true)

    val keys = wordsByInitialLetter.keys.collect().mkString("")

    val expectedResult = "abcdefghiklmnopqrstuwy"
    assert(keys == expectedResult)

  }

}
