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
    implicit def format(useShortFormat: Boolean): String = {
      if (!useShortFormat) {
        val locale = Locale.getDefault
        val formatter = NumberFormat.getNumberInstance(locale)
        formatter.format(num)
      } else {
        f"$num%3.2f"
      }
    }
  }
}
