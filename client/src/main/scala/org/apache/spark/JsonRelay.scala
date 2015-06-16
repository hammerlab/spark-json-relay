package org.apache.spark

import java.io.{Writer, OutputStreamWriter}
import java.net.{Socket, SocketException}
import java.nio.charset.Charset
import javax.net.SocketFactory

import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.JsonProtocol
import org.json4s.JsonAST.{JObject, JNothing, JValue}

class JsonRelay(conf: SparkConf) extends SparkFirehoseListener {

  val appId = conf.get("spark.app.id")
  val host = conf.get("spear.host", "localhost")
  val port = conf.getInt("spear.port", 8123)
  val debugLogs = conf.getBoolean("spear.verbose", false)

  val socketFactory = SocketFactory.getDefault
  var socket: Socket = _
  var writer: Writer = _
  val utf8 = Charset.forName("UTF-8")

  var numReqs = 0
  var lastEvent: Option[String] = None

  def print(s: String): Unit = {
    if (debugLogs) {
      println(s)
    }
  }

  def initSocketAndWriter() = {
    print("*** Initializing socket ***")
    socket = socketFactory.createSocket(host, port)
    writer = new OutputStreamWriter(socket.getOutputStream, utf8)
  }

  initSocketAndWriter()

  override def onEvent(event: SparkListenerEvent): Unit = {
    val jv: JValue = JsonProtocol.sparkEventToJson(event) match {
      case jo: JObject => jo ~ ("appId" -> appId)
      case JNothing => JNothing
      case j => throw new Exception(s"Non-object SparkListenerEvent $j")
    }
    val s: String = compact(jv)

    numReqs += 1
    print(s"Socket To send request: $numReqs\n$s\n")
    try {
      if (socket.isClosed) {
        print(s"*** Socket is closed... ***")
      }
      writer.write(s)
      writer.flush()
    } catch {
      case e: SocketException =>
        socket.close()
        initSocketAndWriter()
        print(s"Socket re-sending: $lastEvent and $s")
        lastEvent.foreach(writer.write)
        writer.write(s)
        writer.flush()
        print("Socket re-sent")
    }

    lastEvent = Some(s)

    print(s"Socket Sending request: $numReqs")
  }
}
