package com.bigdata.flink.proj.testing.taxi.scala

import java.io.{BufferedReader, InputStreamReader, PrintStream}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.util.Random
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/9 4:34 下午 
 * @desc:
 *
 */
object SocketUtil {

  private val random = new Random()

  def log(info:String,printable:Boolean=false): Unit ={
    if(printable){
      println(info)
    }
  }

  def socketServer(port:Int,printable:Boolean=false): Unit ={
    val server = new ServerSocket(port)

    val db = collection.mutable.HashMap(
      1 -> 100,
      2 -> 200,
      3 -> 300,
      4 -> 400,
    )

    while(true){
      val socket = server.accept()
      val runnable = new Runnable {
        override def run(): Unit = {
          val in = socket.getInputStream
          val inputData = new BufferedReader(new InputStreamReader(in)).readLine.toInt
          log(s"receive ${inputData}",printable)
          socket.shutdownInput()
          val value = if(db.contains(inputData)){
            if(inputData % 2 == 0){
              TimeUnit.SECONDS.sleep(2)
            }
            db.get(inputData).get
          }else{
            null
          }

          val out = socket.getOutputStream
          new PrintStream(out).print(value)

          in.close()
          out.close()
          socket.close()
          log(s"send ${value}",printable)
        }
      }
      // TODO 启动线程使用 start
      new Thread(runnable).start()
    }
  }

  // TODO 这里是使用 Future 包裹代码
  def asyncSocketClient(host:String, port:Int, job:Int, printable:Boolean=false): Future[String] = Future{
    val socket = new Socket(host, port)
    val out = socket.getOutputStream
    new PrintStream(out).print(job)
    socket.shutdownOutput()

    log(s"request for ${job}",printable)
    val in = socket.getInputStream
    val str = new BufferedReader(new InputStreamReader(in)).readLine()
    socket.shutdownInput()

    out.close()
    in.close()
    socket.close()
    log(s"response of ${str}",printable)
    str
  }

  def socketClient(host:String, port:Int, job:Int, printable:Boolean=false): String = {
    val socket = new Socket(host, port)
    val out = socket.getOutputStream
    new PrintStream(out).print(job)
    socket.shutdownOutput()

    log(s"request for ${job}",printable)
    val in = socket.getInputStream
    val str = new BufferedReader(new InputStreamReader(in)).readLine()
    socket.shutdownInput()

    out.close()
    in.close()
    socket.close()
    log(s"response of ${str}",printable)
    str
  }



}
