package org.apache.spark

import java.io.{Writer, OutputStreamWriter}
import java.net.{Socket, SocketException}
import java.nio.charset.Charset
import javax.net.SocketFactory

import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.{JObject, JNothing, JValue}

import org.apache.spark.scheduler.{SparkListenerExecutorMetricsUpdate, SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.util.{Utils, JsonProtocol}
import org.apache.spark.ui.OperationGraph

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
    debug("*** JsonRelay: initializing socket ***")
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

    // Parse `SparkListenerJobStart` event to extract DAG, it is converted into JSON string,
    // 'appId' and event as 'SparkListenerDAG' are added
    val maybeDAGs: Seq[String] = event match {
      case jobStart: SparkListenerJobStart => jobStart.stageInfos.map { stageInfo =>
        compact(OperationGraph.makeJsonStageDAG(stageInfo) ~
          ("appId" -> appId) ~ ("Event" -> "SparkListenerSubmitDAG"))
      }
      case otherEvent => Seq.empty
    }

    val s: String = compact(jv)

    numReqs += 1
    if (s.trim().nonEmpty) {
      debug(s"Socket To send request: $numReqs:\n$s\n")
    }
    try {
      if (socket.isClosed) {
        debug(s"*** Socket is closed... ***")
      }
      writer.write(s)
      // write DAGs, if available
      maybeDAGs.foreach(writer.write)
      writer.flush()
    } catch {
      case e: SocketException =>
        socket.close()
        initSocketAndWriter()
        debug(s"*** JsonRelay re-sending: $lastEvent and $s and DAGs ***")
        lastEvent.foreach(writer.write)
        writer.write(s)
        maybeDAGs.foreach(writer.write)
        writer.flush()
        debug("Socket re-sent")
    }

    lastEvent = Some(s)
  }
}
