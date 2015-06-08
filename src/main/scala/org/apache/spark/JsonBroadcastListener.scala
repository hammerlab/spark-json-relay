package org.apache.spark

import com.twitter.io.Charsets
import org.jboss.netty.buffer.ChannelBuffers
import org.json4s.native.Serialization
import org.json4s.DefaultFormats
import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.JsonProtocol
import org.jboss.netty.handler.codec.http.{DefaultHttpRequest, HttpVersion, HttpMethod, HttpRequest, HttpResponse}
import org.json4s.JsonAST.JValue

class JsonBroadcastListener(conf: SparkConf) extends SparkFirehoseListener {

  implicit val formats = org.json4s.DefaultFormats

  val host = conf.get("spear.host", "localhost")
  val port = conf.getInt("spear.port", 8123)

  val client: Service[HttpRequest, HttpResponse] =
    Http.newService(s"$host:$port")

  override def onEvent(event: SparkListenerEvent): Unit = {
    val jv: JValue = JsonProtocol.sparkEventToJson(event)
    val s: String = Serialization.write(jv)
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    request.setContent(ChannelBuffers.copiedBuffer(s, Charsets.Utf16))

    val response: Future[HttpResponse] = client(request)
    response onSuccess { resp: HttpResponse =>
      println("GET success: " + resp)
    } onFailure { t: Throwable =>
      throw new Exception("Request failed", t)
    }
    Await.ready(response)
  }
}
