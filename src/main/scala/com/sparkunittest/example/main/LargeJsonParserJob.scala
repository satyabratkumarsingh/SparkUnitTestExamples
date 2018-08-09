package com.sparkunittest.example.main
import org.apache.spark.sql.SaveMode

object LargeJsonParserJob extends App{
  try {
    val resourcesPath = getClass.getResource("/jsontoparse.json")
    val dsJson = JsonReceiverJob.spark.read.json(resourcesPath.getFile)
    dsJson.write.mode(SaveMode.Overwrite).parquet("/Users/satya/IdeaProjects/SparkUnitTestExamples/src/main/resources/parsedfile")
  } finally {
    WordCountJob.spark.close()
  }

}
