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

package com.codingmaniacs.courses.spark.utils

import java.text.NumberFormat
import java.util.Locale

object StringUtils {

  implicit class LongNumberFormatter(num: Long) {

    implicit def format(): String = {
      val locale = Locale.getDefault
      val formatter = NumberFormat.getNumberInstance(locale)
      formatter.format(num)
    }
  }

  implicit class DoubleNumberFormatter(num: Double) {

    implicit def format(useShortFormat: Boolean): String =
      if (!useShortFormat) {
        val locale = Locale.getDefault
        val formatter = NumberFormat.getNumberInstance(locale)
        formatter.format(num)
      } else {
        f"$num%3.2f"
      }
  }
}
