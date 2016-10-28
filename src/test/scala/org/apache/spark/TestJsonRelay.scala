package org.apache.spark

import java.io.{ByteArrayOutputStream, OutputStreamWriter, Writer}
import java.net.{Socket, SocketException}

/**
 * Test version of Json relay, overwrites `initSocketAndWriter` to provide test-friendly
 * implementation. Both socket and writer are accessible through getters.
 */
private[spark] class TestJsonRelay(conf: SparkConf) extends JsonRelay(conf) {
  private var testStream: ByteArrayOutputStream = _

  def this(appId: String) = this(new SparkConf().set("spark.app.id", appId))

  /** Note that only this method is overwritten, the rest should be testable **/
  override def initSocketAndWriter() {
    // create unconnected socket and writer as byte output stream
    socket = socketFactory.createSocket()
    testStream = new ByteArrayOutputStream()
    writer = new OutputStreamWriter(testStream, utf8)
  }

  def getSocket(): Socket = socket

  /** Set socket with failure, so when it is checked, socket exception is thrown */
  def setCorruptSocket(): Unit = {
    socket = new Socket() {
      override def isClosed(): Boolean = {
        throw new SocketException()
      }

      override def close(): Unit = {
        // do nothing, original impelementation would call 'isClosed' again
      }
    }
  }

  def getWriter(): Writer = writer

  def getTestStream(): ByteArrayOutputStream = testStream

  def getStreamContent(): String = new String(testStream.toByteArray)
}
