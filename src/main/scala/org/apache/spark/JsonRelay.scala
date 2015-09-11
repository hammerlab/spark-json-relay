package org.apache.spark

import java.io.{Writer, OutputStreamWriter}
import java.net.{Socket, SocketException}
import java.nio.charset.Charset
import javax.net.SocketFactory

import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.apache.spark.scheduler.{SparkListenerExecutorMetricsUpdate, SparkListenerEvent}
import org.apache.spark.util.{Utils, JsonProtocol}
import org.json4s.JsonAST.{JObject, JNothing, JValue}

class JsonRelay(conf: SparkConf) extends SparkFirehoseListener {

  val appId = conf.get("spark.app.id")
  val host = conf.get("spark.slim.host", "localhost")
  val port = conf.getInt("spark.slim.port", 8123)
  val debugLogs = conf.getBoolean("spark.slim.verbose", defaultValue = false)

  val socketFactory = SocketFactory.getDefault
  var socket: Socket = _
  var writer: Writer = _
  val utf8 = Charset.forName("UTF-8")

  var numReqs = 0
  var lastEvent: Option[String] = None

  def debug(s: String): Unit = {
    if (debugLogs) {
      println(s)
    }
  }

  def initSocketAndWriter() = {
    debug("*** Initializing socket ***")
    socket = socketFactory.createSocket(host, port)
    writer = new OutputStreamWriter(socket.getOutputStream, utf8)
  }

  initSocketAndWriter()

  override def onEvent(event: SparkListenerEvent): Unit = {
    val jv: JValue = (event match {
      // NOTE(ryan): this duplicates a code path in JsonProtocol in
      // Spark >= 1.5.0. For JsonRelay to use JsonProtocol's code path when
      // running inside Spark <1.5.0, it would have to shade all of
      // spark-core:1.5.0, which seems worse than just duplicating the code
      // path here.
      case e: SparkListenerExecutorMetricsUpdate =>
        if (e.taskMetrics.nonEmpty)
          ("Event" -> Utils.getFormattedClassName(e)) ~
            ("Executor ID" -> e.execId) ~
            ("Metrics Updated" -> e.taskMetrics.map {
              case (taskId, stageId, attemptId, metrics) =>
                ("Task ID" -> taskId) ~
                  ("Stage ID" -> stageId) ~
                  ("Stage Attempt ID" -> attemptId) ~
                  ("Task Metrics" -> JsonProtocol.taskMetricsToJson(metrics))
            })
        else
          JNothing
      case _ =>
        try {
          JsonProtocol.sparkEventToJson(event)
        } catch {
          case e: MatchError =>
            // In Spark 1.5.0 there is a SparkListenerBlockUpdated
            // event type that is only relevant to Spark Streaming jobs; for now we
            // just drop it.
            JNothing
        }
    }) match {
      case jo: JObject => jo ~ ("appId" -> appId)
      case JNothing => JNothing
      case j => throw new Exception(s"Non-object SparkListenerEvent $j")
    }

    val s: String = compact(jv)

    numReqs += 1
    debug(s"Socket To send request: $numReqs\n$s\n")
    try {
      if (socket.isClosed) {
        debug(s"*** Socket is closed... ***")
      }
      writer.write(s)
      writer.flush()
    } catch {
      case e: SocketException =>
        socket.close()
        initSocketAndWriter()
        debug(s"Socket re-sending: $lastEvent and $s")
        lastEvent.foreach(writer.write)
        writer.write(s)
        writer.flush()
        debug("Socket re-sent")
    }

    lastEvent = Some(s)

    debug(s"Socket Sending request: $numReqs")
  }
}
