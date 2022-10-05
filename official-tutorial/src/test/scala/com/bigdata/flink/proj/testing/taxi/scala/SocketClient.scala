package com.bigdata.flink.proj.testing.taxi.scala

import org.junit.Test

import java.io.{BufferedReader, InputStream, InputStreamReader, PrintStream}
import java.net.Socket
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/9 3:08 下午 
 * @desc:
 *
 */
class SocketClient {

  // TODO 这里是使用 Future 包裹代码
  def call(num:Int): Future[String] = Future{
    val socket = new Socket("localhost", 9001)
    val out = socket.getOutputStream
    new PrintStream(out).print(num)
    socket.shutdownOutput()

    val in = socket.getInputStream
    val str = new BufferedReader(new InputStreamReader(in)).readLine()
    socket.shutdownInput()

    out.close()
    in.close()
    socket.close()

    str
  }

  @Test
  def once(): Unit ={
    val future = call(1)
    future.onSuccess{
      case result:String => println(result)
    }
    Await.result(future, Duration.Inf)
  }

  @Test
  def multi(): Unit ={
    val future1 = call(1)
    val future2 = call(2)
    val future3 = call(3)
    future1.onSuccess{
      case result:String => println(result)
    }
    future2.onSuccess{
      case result:String => println(result)
    }
    future3.onSuccess{
      case result:String => println(result)
    }
    Await.result(future2, Duration.Inf)
  }

}
