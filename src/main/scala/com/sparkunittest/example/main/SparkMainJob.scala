package com.sparkunittest.example.main

object SparkMainJob extends App {
  try {
    val resourcesPath = getClass.getResource("/Sentences.txt")
    val dsSentence = WordCountJob.spark.read.text(resourcesPath.getFile)
    val wordsCountDf = WordCountJob.calculateWordCount(dsSentence)
    wordsCountDf.show()
  } finally {
    WordCountJob.spark.close()
  }
}