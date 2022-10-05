package com.flink.study.practice

import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

case class Person(name:String,age:Int)

object Example {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
     Person("Fred", 35),
     Person("Wilma", 35),
     Person("Pebbles", 2))

//    val list = List[Person](Person("Fred", 35), Person("Wilma", 35), Person("Pebbles", 2))
//    val ds = env.fromCollection(list)

//    val ds = env.readTextFile("file").map{line=>
//      val strings = line.split(",")
//      Person(strings(0),strings(1).toInt)
//    }

//    val ds = env.socketTextStream("localhost",9000).map{line=>
//      val strings = line.split(",")
//      Person(strings(0),strings(1).toInt)
//    }


    val adults = ds.filter(_.age >= 18)
    adults.print()
    env.execute("Adult filter")
  }


}
