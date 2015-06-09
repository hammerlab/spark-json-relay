package org.apache.spark

import com.google.common.base.Charsets
import com.twitter.finagle.{Http, Service, SimpleFilter}
import com.twitter.io.Charsets.Utf8
import com.twitter.util.{Await, Future}
import org.apache.spark.util.JsonProtocol
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import org.jboss.netty.handler.codec.http.HttpResponseStatus.{INTERNAL_SERVER_ERROR, FORBIDDEN}
import org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1
import org.jboss.netty.handler.codec.http.{HttpResponseStatus, DefaultHttpResponse, HttpResponse, HttpRequest}
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse

object Main {

  class HandleExceptions extends SimpleFilter[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]) = {

      // `handle` asynchronously handles exceptions.
      service(request) handle { case error =>
        val statusCode = error match {
          case _: IllegalArgumentException =>
            FORBIDDEN
          case _ =>
            INTERNAL_SERVER_ERROR
        }
        val errorResponse = new DefaultHttpResponse(HTTP_1_1, statusCode)
        errorResponse.setContent(copiedBuffer(error.getStackTraceString, Utf8))

        errorResponse
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.app.id", "app")
    val logger = new LoggingListener(conf)
    logger.start()

    val handleExceptions = new HandleExceptions()

    val service =
      handleExceptions andThen
        new Service[HttpRequest, HttpResponse] {
          def apply(req: HttpRequest): Future[HttpResponse] = {
            val content = req.getContent.toString(Charsets.UTF_8)
            println(s"Recv'd ${req.getMethod}: $content, uri: ${req.getUri}")
            if (content.isEmpty) {
              Future.value(
                new DefaultHttpResponse(
                  req.getProtocolVersion,
                  HttpResponseStatus.OK
                )
              )
            } else {
              val jv: JValue = parse(content)
              val event = JsonProtocol.sparkEventFromJson(jv)
              logger.onEvent(event)
              Future.value(
                new DefaultHttpResponse(
                  req.getProtocolVersion,
                  HttpResponseStatus.OK
                )
              )
            }
          }
        }

    val server = Http.serve(":8123", service)
    Await.ready(server)
  }
}
