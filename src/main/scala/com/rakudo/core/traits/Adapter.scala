package com.rakudo.core.traits

import org.apache.flink.streaming.api.scala.DataStream

/**
 * 将 kafka 数据适配成 Map[String, String] 流
 */
trait Adapter {
  def extract(stream: DataStream[String]): DataStream[Map[String, String]]
}