package com.bigdata.flink.proj.testing.taxi.scala

import akka.stream.TLSRole.client

import java.io.{BufferedReader, InputStream, InputStreamReader, PrintStream}
import java.net.ServerSocket
import java.util.concurrent.TimeUnit

/**
 * @author: huhao18@meituan.com
 * @date: 2021/5/9 3:07 下午 
 * @desc:
 *
 */
object SocketServer {

  def main(args: Array[String]): Unit = {
    val server = new ServerSocket(9001)

    val db = collection.mutable.HashMap(
      1 -> 100,
      2 -> 200,
      3 -> 300,
      4 -> 400,
    )

    while(true){
      val socket = server.accept()
      println(s"receive socket from ${socket.getRemoteSocketAddress.toString}")
      val runnable = new Runnable {
        override def run(): Unit = {
          val inputData = new BufferedReader(new InputStreamReader(socket.getInputStream)).readLine.toInt
          socket.shutdownInput()

          val value = if(db.contains(inputData)){
            if(inputData % 2 == 0){
              TimeUnit.SECONDS.sleep(10)
            }
            db.get(inputData).get
          }else{
            null
          }

          new PrintStream(socket.getOutputStream).print(value)
          socket.close()
        }
      }
      // TODO 启动线程使用 start
      new Thread(runnable).start()
    }
  }
}
