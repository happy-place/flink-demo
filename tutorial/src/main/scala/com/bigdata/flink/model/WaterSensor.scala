package com.bigdata.flink.model

case class WaterSensor(id:String,ts:Long,vc:Int)


case class WaterSensor2(id:String, tmstp:Long, vc:Int)
case class T2(id:String, tmstp:Long, vc:Int,windowStart:Long)