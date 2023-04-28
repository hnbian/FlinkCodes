package com.hnbian.flink

import com.alibaba.fastjson.{JSON, JSONObject}

import java.util
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties
import java.util
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONArray


object ESData {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)

    val properties = new Properties()

    val topic = "test1"
    val index = "vehicle3"
    val indexType="_doc"

    properties.setProperty("bootstrap.servers","10.24.5.37:6667,10.24.5.38:6667,10.24.5.39:6667")

    properties.setProperty("group.id","consumer-group1")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset","latest")


    val stream: DataStream[String] =
      env.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), properties))
    //val stream = env.readTextFile("D:\\data.txt")
    // flink 消费数据可以把消费数据的 offset 作为状态保存起来
    // 恢复数据时可以将 offset 设置为恢复数据的位置。

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("10.24.5.37",9200))
    httpHosts.add(new HttpHost("10.24.5.38",9200))
    httpHosts.add(new HttpHost("10.24.5.39",9200))

    var i = 0;
    // 创建 es sink 的 builder
    val esSinkBuilder  = new ElasticsearchSink.Builder[String](
      httpHosts,
      new ElasticsearchSinkFunction[String] {
        /**
         * 发送数据的方法
         * @param t 要发送的数据
         * @param runtimeContext 上下文
         * @param requestIndexer 发送操作请求
         */
        override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          // 将数据包装成json或者Map
          val map = new java.util.HashMap[String,String]()
          i+=1
          println(i)
          val jSONObject = JSON.parseObject(t)
//          println(jSONObject.toString())
//          val capture_time =
//          println(capture_time)
          jSONObject.put("capture_time",jSONObject.getString("capture_time"))
//          println(jSONObject.toString())

          //map.put(String.valueOf(System.nanoTime()),t)

          // 创建 index request 准备发送数据

          val indesRequest = Requests.indexRequest()
            .index(index)
            .`type`(indexType)
            .source(jSONObject)

          // 使用 requestIndexer 发送 HTTP 请求保存数据
          requestIndexer.add(indesRequest)


        }
      })


    stream.addSink(esSinkBuilder.build())
    //stream.print()

    env.execute()
  }

  /**
   * 获取时间格式数据的时间戳
   * @param date 时间格式的字符串
   * @param format 时间格式 默认值yyyy-MM-dd HH:mm:ss，yyyyMMddHHmmss
   * @return 时间戳 毫秒值
   */
  def dateToTimestamp(date:String,format:String="yyyy-MM-dd HH:mm:ss"):Long={
    val simpleDateFormat = new SimpleDateFormat(format)
    simpleDateFormat.parse(date).getTime
  }
}
