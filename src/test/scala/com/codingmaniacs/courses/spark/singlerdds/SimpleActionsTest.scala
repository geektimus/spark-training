package com.codingmaniacs.courses.spark.singlerdds

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.{FunSuite, Matchers}

/**
  * This class contains all the tests related to Actions over Single RDD, RDD[String] and RDD[Int] mostly.
  *
  * Note: This class won't include test for each since it doesn't return any value and if we try to test it we probably
  * get a "Task not serializable" Exception trying to use the assert inside the foreach
  */
class SimpleActionsTest extends FunSuite with SharedSparkContext with Matchers {

  val default_parallelism: Int = 4

  test("basic collect action") {
    val values = List(1, 2, 3, 4, 5)
    val valRDD: RDD[Int] = sc.parallelize(values)

    val expected = "12345"
    val res = valRDD.collect().mkString("")
    assert(res.equals(expected))
  }

  test("basic count action") {
    val values = List(1, 2, 3, 4, 5)
    val valRDD: RDD[Int] = sc.parallelize(values)

    val expected = 5
    val res = valRDD.count()
    assert(res == expected)
  }

  test("basic countByValue action") {
    val values = List(1, 2, 3, 4, 5, 5, 3, 4, 1, 1, 1, 2, 5, 3, 2, 4)
    val valRDD: RDD[Int] = sc.parallelize(values)
    val expected = Map(1 -> 4, 2 -> 3, 3 -> 3, 4 -> 3, 5 -> 3)

    val res = valRDD.countByValue()

    assert(res.equals(expected))

  }

  test("basic take action") {
    val values = List(1, 2, 3, 4, 5)
    val valRDD = sc.parallelize(values)
    val expected = Array(1, 2)

    val res = valRDD.take(2)

    assert(res.sameElements(expected))
  }

  test("basic top (normal) action") {
    val values = List(1, 2, 3, 4, 5)
    val valRDD = sc.parallelize(values)
    val expected = Array(5, 4)

    val res = valRDD.top(2)
    assert(res.sameElements(expected))
  }

  test("basic top (duplicated) action") {
    val values = List(1, 2, 3, 4, 4)
    val valRDD = sc.parallelize(values)
    val expected = Array(4, 4)

    val res = valRDD.top(2)
    assert(res.sameElements(expected))
  }

  test("basic takeOrdered (normal) action") {
    val values = List(1, 2, 3, 4, 4)
    val valRDD = sc.parallelize(values)
    val expected = Array(1, 2)

    val res = valRDD.takeOrdered(2)
    assert(res.sameElements(expected))
  }

  test("basic takeOrdered (reverse) action") {
    val values = List(1, 2, 3, 4)
    val valRDD = sc.parallelize(values)
    val expected = Array(4, 3)

    val res = valRDD.takeOrdered(2)(Ordering[Int].reverse)
    assert(res.sameElements(expected))
  }

  /**
    * Take sample is nondeterministic so we can't validate the values.
    */
  test("basic takeSample action") {
    val values = List(1, 2, 3, 4)
    val valRDD = sc.parallelize(values)
    val numberOfElementsToTake = 2

    val res = valRDD.takeSample(withReplacement = false, numberOfElementsToTake)
    assert(res.length == numberOfElementsToTake)
  }

  test("basic reduce (numbers) action") {
    val values = List(1, 2, 3, 4)
    val valRDD = sc.parallelize(values)

    val res = valRDD.reduce((x, y) => x + y)
    assert(res == 10)
  }

  /**
    * Find the longest word
    */
  //TODO Find a way to find the shortest word using this same string as it contains two words of the same length.
  test("basic reduce (strings) action") {
    val values = "basic operations spark scala java python data"
    val valRDD = sc.parallelize(values.split(" "))


    val res = valRDD.reduce((word_a, word_b) => if (word_a.length > word_b.length) word_a else word_b)
    assert(res == "operations")
  }

  /**
    * In this case the fold behaves in a similar way to the reduce function
    * The value passed to fold should be the identity for the operation 0 for + and 1 for *
    */
  test("basic fold action 01 (similar to reduce [sum])") {
    val values = List(1, 2, 3, 4)
    val valRDD = sc.parallelize(values, default_parallelism)
    val identity = 0

    val res = valRDD.fold(identity)((x, y) => x + y)
    assert(res == 10)
  }

  /**
    * In this case the fold behaves in a similar way to the reduce function
    */
  test("basic fold action 02 (similar to reduce [multiplication])") {
    val values = List(1, 2, 3, 4)
    val valRDD = sc.parallelize(values, default_parallelism)
    val identity = 1

    val res = valRDD.fold(identity)((x, y) => x * y)
    assert(res == 24)
  }

  /**
    * This is what happens when you provide a not identity value for the operation.
    */
  test("basic fold action 03") {
    val values = List(1, 2, 3, 4)
    val valRDD = sc.parallelize(values, default_parallelism)
    val identity = 2

    val res = valRDD.fold(identity)((x, y) => x + y)
    assert(res == 20)
  }

  /**
    * Note here the Partition length is 4 (Given the cores on this box).
    * So in each partition, after the RDD elements are added, the 'Zero value of 0.5' is also added (ie 0.5 * 4 = 2).
    * When the result of each partition is added, once again a value of 0.5 is added to the result.
    * Therefore we have ((0.5 * 4) + 0.5), totally 2.5
    *
    * We can say that in this case the expected value is:
    * - (identity*num-partitions)+identity + reduce ((x,y) => x + y)
    */
  test("basic fold action 04 (sum)") {
    val values = List(("maths", 4.2), ("science", 3.8))
    val valRDD = sc.parallelize(values, default_parallelism)

    val identity = ("extra", 0.5)

    val res = valRDD.fold(identity) { (acc, marks) =>
      val sum = acc._2 + marks._2
      ("total", sum)
    }

    assert(res._2 == 10.5)
  }

  /**
    * Note here the Partition length is 4 (Given the cores on this box).
    * So in each partition, after the RDD elements are multiplied, the 'Zero value of 0.5' is also multiplied (ie pow(0.5, 4) = 0.0625).
    * When the result of each partition is added, once again a value of 0.5 is multiplied to the result.
    * Therefore we have (pow(0.5 , 5), totally 0.03125
    *
    * We can say that in this case the expected value is:
    * - pow(identity, num-partitions + 1) * reduce ((x,y) => x * y)
    */
  test("basic fold action 05 (multiplication)") {
    val Eps = 1e-3

    val values = List(("maths", 4.2), ("science", 3.8))
    val valRDD = sc.parallelize(values, default_parallelism)
    val identity = ("extra", 0.5)

    val expected = 0.49875

    val res = valRDD.fold(identity) { (acc, marks) =>
      val mul = acc._2 * marks._2
      ("total", mul)
    }

    res._2 should be(expected +- Eps)
  }

  /**
    * The result of the fold (with a non identity value) vs the result of the reduce
    */
  test("basic fold action 06 (reduce vs fold (non identity value)") {
    val values = List(("maths", 4.2), ("science", 3.8))
    val valRDD = sc.parallelize(values, default_parallelism)

    valRDD.persist()

    val identity = ("extra", 2.0)

    val resFold = valRDD.fold(identity) { (acc, marks) =>
      val sum = acc._2 + marks._2
      ("total", sum)
    }

    val resReducer = valRDD.reduce((x, y) => ("total", x._2 + y._2))._2
    val numPartitions = valRDD.partitions.length

    assert(resFold._2 == ((identity._2 * numPartitions) + identity._2 + resReducer))

    valRDD.unpersist()
  }

  test("basic aggregate action") {
    val values = List(1, 2, 3, 3)
    val valRDD = sc.parallelize(values)
    val zeroValue = (0, 0)

    val res = valRDD.aggregate(zeroValue)(
      (accumulator, value) => (accumulator._1 + value, accumulator._2 + 1),
      (accumulator_1, accumulator_2) => (accumulator_1._1 + accumulator_2._1, accumulator_1._2 + accumulator_2._2)
    )

    val expected = (9, 4)

    assert(res._1 == expected._1 && res._2 == expected._2)

  }
}
