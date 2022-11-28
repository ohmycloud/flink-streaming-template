package com.rakudo.module

import com.google.inject.{AbstractModule, Provides, Singleton}
import com.rakudo.StreamingApp
import com.rakudo.conf.KafkaConfiguration
import com.rakudo.core.impl.AdapterImpl
import com.rakudo.core.traits.Adapter
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.connector.kafka.source.KafkaSource
import java.util.Properties
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.eventtime.WatermarkStrategy

object MainModule extends AbstractModule {

  override def configure(): Unit = {

    bind(classOf[KafkaConfiguration]).asEagerSingleton()   // Kafka 配置
    bind(classOf[Adapter]).toInstance(AdapterImpl)         // Kafka 数据适配
    bind(classOf[StreamingApp])                            // 程序入口
  }

  /**
    * 读取 Kafka 数据并解析为 DataFrame
    * @return
    */
  @Provides
  @Singleton
  def KafkaDataSource(kafkaConf: KafkaConfiguration): DataStream[String] = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", kafkaConf.bootstrapServers)

    import scala.collection.JavaConversions.seqAsJavaList

    val source = KafkaSource.builder()
      .setBootstrapServers(kafkaConf.bootstrapServers)
      .setTopics(kafkaConf.topics.split(",").toList)
      .setGroupId(kafkaConf.groupId)
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema)
      .build()

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-stream")
  }
}
