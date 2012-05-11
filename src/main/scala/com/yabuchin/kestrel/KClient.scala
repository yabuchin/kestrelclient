package com.yabuchin.kestrel

import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Client
import com.twitter.finagle.kestrel.ReadHandle
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.service.Backoff
import com.twitter.util.Duration
import com.twitter.util.Time
import com.twitter.util.JavaTimer

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
 * @param hosts
 */
class KClient(val hosts: Seq[String]) {

  def this(host:String) = this(Seq(host))

  private val stopped = new AtomicBoolean(false)

  private val NumHosts = hosts.size

  private val clients: Seq[Client] = hosts map {
    host =>
      Client(ClientBuilder()
        .codec(Kestrel())
        .hosts(host)
        .hostConnectionLimit(1)
        .buildFactory())
  }

  private def getRandomHostIndex:Int = scala.util.Random.nextInt(NumHosts)

  def deletQueue(queuename: String) = clients.foreach(_.delete(queuename))

  def set(key:String, v:String){
    val value = ChannelBuffers.wrappedBuffer(v.getBytes)
    clients(getRandomHostIndex).set(key, value, Time.fromMilliseconds(0))
  }

  def get(key:String): String = get(key, 0)

  def get(key:String, waitTime: Int): String = {
    val waitUpTo = Duration(waitTime, TimeUnit.MILLISECONDS)
    clients(getRandomHostIndex).get(key, waitUpTo)() match {
      case Some(value) => value.toString(CharsetUtil.UTF_8)
      case None => throw new NoSuchElementException
    }
  }

  def read = {
    val readHandles: Seq[ReadHandle] = {
      val queueName = "foo"
      val timer = new JavaTimer(isDaemon = true)
      val retryBackoffs = Backoff.const(10.milliseconds)
      clients map {
        _.readReliably(queueName, timer, retryBackoffs)
      }
    }

    val readHandle: ReadHandle = ReadHandle.merged(readHandles)

    // Attach an async error handler that prints to stderr
    readHandle.error foreach {
      e =>
        if (!stopped.get) System.err.println("zomg! got an error " + e)
    }

    // Attach an async message handler that prints the messages to stdout
    readHandle.messages map {
      msg =>
        try {
          msg.bytes.toString(CharsetUtil.UTF_8)
        } finally {
          msg.ack.sync() // if we don't do this, no more msgs will come to us
        }
    }
    close(readHandle)
  }

  private def close(readHandle: ReadHandle){
    readHandle.close
    close
  }

  def close{
    stopped.set(true)
    clients foreach {
      _.close()
    }
  }
}
