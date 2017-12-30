package com.sparkunittest.example.kafka.producer

import com.sparkunittest.example.kafka.config.KafkaConfig
import com.sparkunittest.example.models.Message

object KafkaMessageProducerLauncher extends App {

  val kafkaMessageProducer = KafkaMessageProducer(KafkaConfig.topic, KafkaConfig.hostName + ":" + KafkaConfig.port)
  var i = 0
  while (true) {
    i = i + 1
    val message = Message("Message_Id" + i, "Message_Body" + i)
    kafkaMessageProducer.sendToTopic(message)
    Thread.sleep(1000)
  }

}
