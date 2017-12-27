package com.sparkunittest.example.kafka.config

object KafkaConfig {

  import com.typesafe.config.Config
  import com.typesafe.config.ConfigFactory

  val conf: Config = ConfigFactory.load
  def hostName = {conf.getString("host")}
  def port = {conf.getString("port")}
  def topic = {conf.getString("topic")}
  def zookeeperHost = {conf.getString("zookeeperHost")}

}
