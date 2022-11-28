package com.rakudo.conf

import com.google.inject.Singleton
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Kafka 配置
  */
@Singleton
class KafkaConfiguration extends Serializable {
  private val config:           Config  = ConfigFactory.load()
  lazy    val kafkaConfig:      Config  = config.getConfig("kafka")
  lazy    val bootstrapServers: String  = kafkaConfig.getString("bootstrap.servers") // bootstrap servers 的地址
  lazy    val topics:           String  = kafkaConfig.getString("topics")            // 订阅的 topic 名, 多个 topic 用逗号分隔
  lazy    val groupId:          String  = kafkaConfig.getString("group.id")          // 消费者组 ID
}