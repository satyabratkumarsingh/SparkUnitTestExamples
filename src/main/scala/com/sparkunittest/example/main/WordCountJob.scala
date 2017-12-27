package com.sparkunittest.example.main

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.count

object WordCountJob extends SparkExampleSession {
  def calculateWordCount(dsSentence: DataFrame): DataFrame = {
    import spark.implicits._
    dsSentence.as[String].flatMap(_.split(' ')).map(_.replace(",", "")).map(_.replace(".", "")).
      filter(!_.isEmpty).toDF().groupBy("value")
      .agg(count("*") as "word-count")

  }
}