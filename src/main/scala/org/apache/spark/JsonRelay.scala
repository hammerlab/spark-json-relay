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

  // Executor thread dump event
  val SparkListenerExecutorThreadDumpEvent = "SparkListenerExecutorThreadDumpEvent"

  def debug(s: String): Unit = {
    if (debugLogs) {
      println(s)
    }
  }

  def initSocketAndWriter() = {
    println("*** JsonRelay: initializing socket ***")
    socket = socketFactory.createSocket(host, port)
    writer = new OutputStreamWriter(socket.getOutputStream, utf8)
  }

  /** Get thread dump for executor, `executorId` can be numeric id or "driver" */
  def jsonExecutorThreadDump(executorId: String): Array[JObject] = {
    val traces = SparkContext.getOrCreate().getExecutorThreadDump(executorId).getOrElse(Array.empty)
    if (traces.isEmpty) {
      Array.empty
    } else {
      traces.map { threadTrace =>
        ("Event" -> SparkListenerExecutorThreadDumpEvent) ~
          ("executorId" -> executorId) ~
          ("threadId" -> threadTrace.threadId) ~
          ("threadName" -> threadTrace.threadName) ~
          ("threadState" -> threadTrace.threadState.toString) ~
          ("stackTrace" -> threadTrace.stackTrace)
      }
    }
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

    // == Executor thread dump ==
    // This code collects thread dumps from executors and driver on each executor metric update,
    // this allows updates to be fairly frequent, also on every executor update we fetch driver
    // thread dump. Note that this can potentially be a performance bottleneck.
    val threadDumps: Array[JObject] = event match {
      case e: SparkListenerExecutorMetricsUpdate =>
        // Option of array of thread dump objects, if executor is unreachable, or does not exist
        // this logs error message in stderr and returns None.
        // Also append data for driver, since there is no corresponding event for it
        (jsonExecutorThreadDump(e.execId) ++ jsonExecutorThreadDump("driver")).map { each =>
          each ~ ("appId" -> appId)
        }
      case other =>
        Array.empty
    }

    val s: String = compact(jv)
    val threadDumpJson: Array[String] = threadDumps.map(compact)

    numReqs += 1
    if (s.trim().nonEmpty) {
      debug(s"Socket To send request: $numReqs:\n$s\n")
    }
    try {
      if (socket.isClosed) {
        debug(s"*** Socket is closed... ***")
      }
      writer.write(s)
      threadDumpJson.foreach(writer.write)
      writer.flush()
    } catch {
      case e: SocketException =>
        socket.close()
        initSocketAndWriter()
        println(s"*** JsonRelay re-sending: $lastEvent and $s ***")
        lastEvent.foreach(writer.write)
        writer.write(s)
        println(s"*** JsonRelay re-sending: thread dumps ***")
        threadDumpJson.foreach(writer.write)
        writer.flush()
        debug("Socket re-sent")
    }

    lastEvent = Some(s)
  }
}
