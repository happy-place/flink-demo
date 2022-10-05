package com.bigdata.flink.proj.testing.taxi.scala

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/9 6:07 下午 
 * @desc:
 *
 */

import org.junit.Test

import scala.util.{Failure, Random, Success, Try}
import scala.concurrent.{Await, Future, future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

case class Water(temperature: Int)

case class GrindingException(msg: String) extends Exception(msg)

case class FrothingException(msg: String) extends Exception(msg)

case class WaterBoilingException(msg: String) extends Exception(msg)

case class BrewingException(msg: String) extends Exception(msg)



class FutureTest {

  type CoffeeBeans = String
  type GroundCoffee = String
  type Milk = String
  type FrothedMilk = String
  type Espresso = String
  type Cappuccino = String

  def grind(beans: CoffeeBeans): Future[GroundCoffee] = Future {
    println("start grinding...")
    Thread.sleep(Random.nextInt(2000))
    if (beans == "baked beans") throw GrindingException("are you joking?")
    println("finished grinding...")
    s"ground coffee of $beans"
  }

  def heatWater(water: Water): Future[Water] = Future {
    println("heating the water now")
    Thread.sleep(Random.nextInt(2000))
    println("hot, it's hot!")
    water.copy(temperature = 85)
  }

  def frothMilk(milk: Milk): Future[FrothedMilk] = Future {
    println("milk frothing system engaged!")
    Thread.sleep(Random.nextInt(2000))
    println("shutting down milk frothing system")
    s"frothed $milk"
  }

  def brew(coffee: GroundCoffee, heatedWater: Water): Future[Espresso] = Future {
    println("happy brewing :)")
    Thread.sleep(Random.nextInt(2000))
    println("it's brewed!")
    "espresso"
  }

  def combine(espresso: Espresso, frothedMilk: FrothedMilk): Cappuccino = "cappuccino"

  def temperatureOkay(water: Water): Future[Boolean] = Future {
    (80 to 85) contains (water.temperature)
  }

  // 成功回调
  @Test
  def onSuccess(): Unit ={
    val future = grind("arabica beans")
    future.onSuccess { case ground =>
      println("okay, got my ground coffee")
    }
    Await.result(future,Duration.Inf)
  }

  // 完成回调，包含成功和失败，且失败会触发回调，然后在被调用位置跑出异常
  @Test
  def onComplete(): Unit ={
    val future = grind("baked beans")
    future.onComplete {
      case Success(ground) => println(s"got my $ground")
      case Failure(ex) => println("This grinder needs a replacement, seriously!")
    }
    Await.result(future,Duration.Inf)
  }

  // 获取 map 消费一个 future 内容，产生新 future
  @Test
  def map(): Unit ={
    val tempreatureOkay: Future[Boolean] = heatWater(Water(25)) map { water =>
      println("we're in the future!")
      (80 to 85) contains (water.temperature)
    }
    Await.result(tempreatureOkay,Duration.Inf) // 此处不阻塞的话，结果为 Future(<not completed>)
    println(tempreatureOkay)
  }

  // 消费一个 future 内容，产生一个新 嵌套 Future，通过 flatMap，剥掉一层
  @Test
  def flatMap(): Unit ={
    // 不使用 flatMap 会出现嵌套 Future
    val nestedFuture: Future[Future[Boolean]] = heatWater(Water(25)) map {
      water => temperatureOkay(water)
    }

    val flatFuture: Future[Boolean] = heatWater(Water(25)) flatMap {
      water => temperatureOkay(water)
    }

    Await.result(flatFuture,Duration.Inf)

    println(nestedFuture) // Future(Success(Future(Success(true))))
    println(flatFuture) // Future(Success(true))
  }

  // 通过 for 组织多个 异步回调，前后存在依赖关系时，就串行执行
  @Test
  def depsInFor(): Unit ={
    // for 进行链式调用
    val acceptable: Future[Boolean] = for {
      heatedWater <- heatWater(Water(25))
      okay <- temperatureOkay(heatedWater)
    } yield okay

    Await.result(acceptable,Duration.Inf)
    println(acceptable)
  }

  // 通过 for 组织多个 异步回调，前后不存在依赖关系时，就串并行执行
  def noDepsInFor(): Unit = {
    val groundCoffee = grind("arabica beans")
    val heatedWater = heatWater(Water(20))
    val frothedMilk = frothMilk("milk")
    val future = for {
      ground <- groundCoffee
      water <- heatedWater
      foam <- frothedMilk
      espresso <- brew(ground, water)
    } yield combine(espresso, foam)

    Await.result(future,Duration.Inf)
    println(future)

  }


}
