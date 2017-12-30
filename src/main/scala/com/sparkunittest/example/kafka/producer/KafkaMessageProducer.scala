package com.sparkunittest.example.kafka.producer

import java.util.{Properties, UUID}

import com.sparkunittest.example.models.Message
import com.sparkunittest.example.serializer.MessageSerializer
import kafka.message.{DefaultCompressionCodec, NoCompressionCodec}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

case class KafkaMessageProducer(
                                 topic: String,
                                 brokerList: String,
                                 clientId: String = UUID.randomUUID().toString,
                                 synchronously: Boolean = true,
                                 compress: Boolean = true,
                                 batchSize: Integer = 200,
                                 messageSendMaxRetries: Integer = 3,
                                 requestRequiredAcks: Integer = -1
                               ) {

  val props = new Properties()
  val codec = if(compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.ByteArraySerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, Array[Byte]](props)


  def sendToTopic(message:  Message): Unit ={
    try {
      val messageToSend = new ProducerRecord[String, Array[Byte]](topic, null, MessageSerializer.serialize(message))
      producer.send(messageToSend)
      println("Message submitted")
    } catch {
      case e: Exception =>
        e.printStackTrace
        System.exit(1)
    }
  }
}
