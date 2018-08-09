package com.sparkunittest.example.main

import org.apache.spark.sql.{DataFrame, SaveMode}


object JsonReceiverJob extends SparkExampleSession {

  def formatJson(dsSentence: DataFrame) : DataFrame = {

    dsSentence

  }
}