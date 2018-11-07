package com.codingmaniacs.courses.spark.utils

import java.text.NumberFormat
import java.util.Locale

object StringUtils {
  implicit class NumberFormatter(num: Long) {
    implicit def format(): String = {
      val locale = Locale.getDefault
      val formatter = NumberFormat.getNumberInstance(locale)
      formatter.format(num)
    }
  }
}
