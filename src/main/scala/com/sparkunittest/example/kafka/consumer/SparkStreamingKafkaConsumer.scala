package com.sparkunittest.example.kafka.consumer
import com.sparkunittest.example.kafka.config.KafkaConfig
import com.sparkunittest.example.main.SparkExampleSession
import com.sparkunittest.example.serializer.MessageSerializer
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import com.sparkunittest.example.models.Message

object SparkStreamingKafkaConsumer extends App with SparkExampleSession{

   startKafkaConsumer()

  def startKafkaConsumer() = {

    spark.conf.set("spark.driver.allowMultipleContexts", "true")
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer ],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array(KafkaConfig.topic)
    val stream = KafkaUtils.createDirectStream[String, Array[Byte]](
      streamingContext,
      PreferConsistent,
      Subscribe[String, Array[Byte]](topics, kafkaParams)
    )
    stream
      .map(x=>
      {
        val message = MessageSerializer.deserialize(x.value()).asInstanceOf[Message]
        println(message.messageBody)
      }).print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
