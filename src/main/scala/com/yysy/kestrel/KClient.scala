package com.yysy.kestrel

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.{Client, ReadHandle}
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.util.{Duration, Time}

import java.util.NoSuchElementException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.TimeUnit

import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.buffer.ChannelBuffers

/**
 * Created with IntelliJ IDEA.
 * User: Yuichi Yabu
 * Date: 12/05/11
 * Time: 11:40
 * To change this template use File | Settings | File Templates.
 */

/**
 * host is pair hostname with port (e.g. localhost:22133 )
 * @param host
 */
class KClient(val host: String) {

  private val stopped = new AtomicBoolean(false)

  private val client =
    Client(ClientBuilder()
      .codec(Kestrel())
      .hosts(host)
      .hostConnectionLimit(1)
      .buildFactory())

  def deletQueue(queueName: String) = client.delete(queueName)

  def set(queueName: String, v: String) {
    val value = ChannelBuffers.wrappedBuffer(v.getBytes)
    client.set(queueName, value, Time.fromMilliseconds(0))
  }

  def get(queueName: String): String = get(queueName, 0)

  def get(queueName: String, waitTime: Int): String = {
    val waitUpTo = Duration(waitTime, TimeUnit.MILLISECONDS)
    client.get(queueName, waitUpTo)() match {
      case Some(value) => value.toString(CharsetUtil.UTF_8)
      case None => throw new NoSuchElementException
    }
  }

  def read(queueName: String) = {

    val readHandle = client.readReliably(queueName)

    readHandle.error foreach {
      e =>
        if (!stopped.get) System.err.println("zomg! got an error " + e)
    }

    readHandle.messages map {
      msg =>
        try {
          println(msg.bytes.toString(CharsetUtil.UTF_8))
        } finally {
          msg.ack.sync()
        }
    }
    close(readHandle)
  }

  private def close(readHandle: ReadHandle) {
    readHandle.close
  }

  def close {
    stopped.set(true)
    client.close
  }
}
