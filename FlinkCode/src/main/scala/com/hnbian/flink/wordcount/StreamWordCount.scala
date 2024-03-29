package com.hnbian.flink.wordcount


import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//com.hnbian.flink.wordcount.StreamWordCount
/**
  * @Author haonan.bian
  * @Description 数据流的WordCount
  * @Date 2020-04-28 22:31
  *       发送数据
  *       nc -lk 8888
  **/
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    //--host localhost --port 8888
//    val parameters = ParameterTool.fromArgs(args)
//
//    val host = parameters.get("host")
//
//    val port = parameters.getInt("port")

//    val parameters = ParameterTool.fromArgs(args)

    val host = "10.24.5.200"

    val port = 8888
    println(s"host:$host")
    println(s"port:$port")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val inputDataStream = env.socketTextStream(host, port)

    inputDataStream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print()
    //.setParallelism(2) // 设置并行度 默认CPU线程数

    // 执行作业
    env.execute("StreamWordCount")
  }

  /**
    * 前面的数字是线程编号
    *
    * 11> (xiah,1)
    * 9> (xiaog,1)
    * 7> (xiaom,1)
    * 4> (hello,1)
    * 4> (hello,2)
    * 4> (hello,3)
    * 9> (xiaod,1)
    * 4> (hello,4)
    */
}
