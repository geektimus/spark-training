# Spark Certification Preparation 

[![Build Status](https://travis-ci.org/geektimus/spark-training.svg?branch=master)](https://travis-ci.org/geektimus/spark-training)

## Chapter 1: Introduction to Data Analysis with Spark
## Chapter 2: Downloading Spark and Getting Started
## Chapter 3: Programming with RDDs

### Exercises

**TextAnalyzer:** Find the top n most common words in a text file

```shell
java -jar target/spark-certification-1.0-SNAPSHOT-dev.jar text-file.txt  number-of-words
```

Notes:
* We need to improve the parameter handling in the main class.

### Tests

To run the tests we just run.
```mvn test```