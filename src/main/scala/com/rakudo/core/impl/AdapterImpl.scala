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
          val map = new mutable.HashMap[String,String]()
          val jsonData: JSONObject = JSON.parseObject(d)
          jsonData.keySet().toArray.foreach( f => {
            val field: String = f.toString
            val value: String = jsonData.getString(field)
            if ("" != value && "" != field) {
              map.put(field, jsonData.getString(field))
            }
          })
          if (map.nonEmpty) {
            map.toMap
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
