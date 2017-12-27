package com.sparkunittest.example.main

import org.apache.spark.sql.SparkSession

trait SparkExampleSession {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Spark Example for Unit tests")
      .getOrCreate()
  }
}
