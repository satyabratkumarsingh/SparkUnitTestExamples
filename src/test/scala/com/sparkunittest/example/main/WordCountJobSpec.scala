package com.sparkunittest.example.main

import org.scalatest.FunSpec

class WordCountJobSpec extends FunSpec with SparkExampleSession {

  import spark.implicits._
  it("tests word count in spark") {

    val sentencesDf = Seq("This is  Satya . You should be knowing my name until now, its Satya , again I would say Satya ."
    ).toDF()

    val wordCountDf = WordCountJob.calculateWordCount(sentencesDf)
    assert(wordCountDf.count() == 16)
    assert(wordCountDf.filter("value = 'Satya'").first().getAs[Long]("word-count") == 3)

  }
}