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

package com.codingmaniacs.courses.spark.custom.text

import com.codingmaniacs.courses.spark.text.TextAnalyzer
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TextAnalyzerTest extends AnyFunSuite with SharedSparkContext with Matchers {

  test("The text analyzer should count the number of words in a text") {
    val randomText: String =
      """Excited him now natural saw passage offices you minuter. At by asked being court hopes.
         |Farther so friends am to detract. Forbade concern do private be. Offending residence
         |but men engrossed shy. Pretend am earnest offered arrived company so on. Felicity
         |informed yet had admitted strictly how you.
      """.trim.stripMargin

    val textRDD = sc.parallelize(randomText.split("\n"))

    val result = TextAnalyzer.wordCountInText(textRDD)

    val expected = 48 //Non unique words

    assert(result.equals(expected))
  }

  test("The text analyzer should find the top N words in a text") {
    val randomText: String =
      """
        |Bilbo drew his hand over his eyes. I am sorry, he said. But I felt so
        |queer. And yet it would be a relief in a way not to be bothered with it any
        |more. It has been so growing on my mind lately. Sometimes I have felt it was
        |like an eye looking at me. And I am always wanting to put it on and
        |disappear, don't you know; or wondering if it is safe, and pulling it out to
        |make sure. I tried locking it up, but I found I couldn't rest without it in
        |my pocket. I don't know why. And I don't seem able to make up my mind.
      """.trim.stripMargin

    val textRDD = sc.parallelize(randomText.split("\n"))

    val result = TextAnalyzer.calculateTopNWordsInText(textRDD, 4)

    val expected = Array(("don't", 3), ("make", 2), ("felt", 2), ("mind", 2))

    result should contain allElementsOf expected
  }

  test(
    "The text analyzer should group the lines by the first word and count the number of appearances"
  ) {
    val logLines: String =
      """
        |INFO: Dummy Info 1
        |INFO: Dummy Info 2
        |ERROR: Dummy Error 1
        |INFO: Dummy Info 3
        |INFO: Dummy Info 4
        |ERROR: Dummy Error 2
        |WARN: Dummy WARN 1
        |INFO: Dummy Info 5
      """.trim.stripMargin

    val regex = """^([A-Z]+):.*""".r

    val textRDD = sc.parallelize(logLines.split("\n"))

    val result = TextAnalyzer.countDataPerRowInText(regex, textRDD)

    val expected = Array(("INFO", (5, 62.5)), ("ERROR", (2, 25)), ("WARN", (1, 12.5)))

    assert(result.sameElements(expected))
  }

  test("The text analyzer should group the lines by the regex and count the number of appearances") {
    val logLines: String =
      """
        |INFO: Dummy Info 1
        |INFO: Dummy Info 2
        |ERROR: Dummy Error 1
        |INFO: Dummy Info 3
        |INFO: Dummy Info 4
        |ERROR: Dummy Error 2
        |WARN: Dummy WARN 1
        |INFO: Dummy Info 5
      """.trim.stripMargin

    val regex = """^([A-Z]+):.*""".r

    val contentRDD = sc.parallelize(logLines.split("\n"))

    val result = TextAnalyzer.countDataPerRowInText(regex, contentRDD)

    val expected = Array(("INFO", (5, 62.5)), ("ERROR", (2, 25)), ("WARN", (1, 12.5)))

    assert(result.sameElements(expected))
  }

  test(
    "The text analyzer should group the lines by the regex and count the number of appearances matching the regex"
  ) {
    val logLines: String =
      """
        |INFO: Dummy Info 1
        |INFO: Dummy Info 2
        |ERROR: Dummy Error 1
        |INFO: Dummy Info 3
        |INFO: Dummy Info 4
        |ERROR: Dummy Error 2
        |WARN: Dummy WARN 1
        |INFO: Dummy Info 5
      """.trim.stripMargin

    val regex = """^.*:\s([a-zA-Z]+)\s.*""".r

    val contentRDD = sc.parallelize(logLines.split("\n"))

    val result = TextAnalyzer.countDataPerRowInText(regex, contentRDD)

    val expected = Array(("Dummy", (8, 100)))

    assert(result.sameElements(expected))
  }
}
