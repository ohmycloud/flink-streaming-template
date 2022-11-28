package com.rakudo.core.impl

import com.alibaba.fastjson.{JSON, JSONObject}
import com.rakudo.core.traits.Adapter
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import scala.collection.mutable

object AdapterImpl extends Adapter {
  override def extract(stream: DataStream[String]): DataStream[Map[String, String]] = {
    stream.map { d =>
      if (null != d) {
        try {
          // 存放 JSON 中的 key, value 键值对儿
          val chargingPileDataMap = new mutable.HashMap[String,String]()
          val chargingPileData: JSONObject = JSON.parseObject(d)
          chargingPileData.keySet().toArray.foreach( f => {
            val field: String = f.toString
            val value: String = chargingPileData.getString(field)
            if ("" != value && "" != field) {
              chargingPileDataMap.put(field, chargingPileData.getString(field))
            }
          })
          if (chargingPileDataMap.nonEmpty) {
            chargingPileDataMap.toMap
          } else {
            Map("" -> "")
          }
        } catch {
          case e: Exception => println(e, d)
            Map("" -> "")
        }
      } else {
        Map("" -> "")
      }
    }.filter(x => x.nonEmpty)
  }
}
