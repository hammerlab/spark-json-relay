package org.apache.spark

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.http.{RequestBuilder, Http}
import org.json4s.jackson.JsonMethods._
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.JsonProtocol
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}
import org.json4s.JsonAST.JValue
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer

class JsonBroadcastListener(conf: SparkConf) extends SparkFirehoseListener {

  val host = conf.get("spear.host", "localhost")
  val port = conf.getInt("spear.port", 8123)
  val retries = conf.getInt("spear.retries", 2)
  val connectionLimit = conf.getInt("spear.connection-limit", 1)

  val client: Service[HttpRequest, HttpResponse] =
    ClientBuilder()
      .codec(Http())
      .hosts(s"$host:$port")
      .hostConnectionLimit(connectionLimit)
      .retries(retries)
      .name("json-client")
      .build()

  override def onEvent(event: SparkListenerEvent): Unit = {
    val jv: JValue = JsonProtocol.sparkEventToJson(event)
    val s: String = compact(jv)
//    println(s"Sending: $s")
    val payload = s.getBytes("UTF-8")
    val request =
      RequestBuilder().url(s"http://$host:$port/")
        .setHeader("Content-Type", "text/json")
        .setHeader("Content-Length", payload.length.toString)
        .buildPost(wrappedBuffer(payload))

    val response: Future[HttpResponse] = client(request)
    response onSuccess { resp: HttpResponse =>
//      println("GET success: " + resp)
    } onFailure { t: Throwable =>
      throw new Exception("Request failed", t)
    }
    Await.ready(response)
  }
}
