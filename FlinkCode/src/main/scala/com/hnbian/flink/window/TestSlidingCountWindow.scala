package com.hnbian.flink.window

import com.hnbian.flink.common.Record

/**
  * @Author haonan.bian
  * @Description 滑动计数窗口
  * @Date 2020/8/15 23:41 
  **/
object TestSlidingCountWindow {
  def main(args: Array[String]): Unit = {

    import org.apache.flink.streaming.api.scala._
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream1: DataStream[String] = env.socketTextStream("localhost",9999)

    val stream2: DataStream[Record] = stream1.map(data => {
      val arr = data.split(",")
      Record(arr(0), arr(1), arr(2).toInt)
    })

    // 取出 2 条记录之内,每个 classId 年纪最小的用户
    stream2.map(record=>{
      (record.classId,record.age)
    }).keyBy(_._1)
      .countWindow(4,2) //  窗口数量是 4 滑动为 2 ， 默认使用的是 processing time
      .reduce((r1,r2)=>{(r1._1,r1._2.min(r2._2))})
      .print("minAge")

    env.execute()
  }
}
