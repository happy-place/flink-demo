package com.bigdata.flink.proj.common.func

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util
import scala.concurrent.ExecutionContext.Implicits.global
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

object CommonUtil {

  def getClassPath(path: String): String = {
    getClass.getClassLoader.getResource(path).getPath
  }

  def getResourcePath(path: String): String = {
    val root = getClass.getClassLoader.getResource("./").getPath.split("target")(0)
    s"${root}src/main/resources/${path}"
  }

  def sendMessage(topic:String, messages:List[JSONObject], primaryKey:String) = {
    val func = () => {
      val props = new Properties()
      props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
      props.put("zookeeper.connect", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val producer = new KafkaProducer[String, String](props)
      val partitions = producer.partitionsFor(topic).size()

      for(msg <- messages){
        val json = msg.toJSONString
        val key = math.abs(msg.get(primaryKey).hashCode).toString
        val partition = math.abs(msg.get(primaryKey).hashCode) % partitions
        val record = new ProducerRecord[String, String](topic, partition, null, key, json)
        producer.send(record)
      }

      producer.flush()
      producer.close()
    }
    func
  }

  def runAsyncJob(func:Function0[Unit], waitForSec:Long=0L): Future[Unit] = Future {
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        func()
      }
    })
    TimeUnit.SECONDS.sleep(waitForSec)
    thread.start()
  }


}
