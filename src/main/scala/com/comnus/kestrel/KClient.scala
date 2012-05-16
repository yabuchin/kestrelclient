package com.comnus.kestrel

import collection.mutable.ListBuffer

import com.twitter.conversions.time.intToTimeableNumber
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.kestrel.{ReadMessage, Client}
import com.twitter.finagle.service.Backoff
import com.twitter.util.{JavaTimer, Time}

import java.util.concurrent.atomic.AtomicBoolean

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.util.CharsetUtil

/**
 * host is pair hostname with port (e.g. localhost:22133 )
 * @param host
 */
class KClient(host: String, hostConnectionLimit: Int) {

  private val client =
    Client(ClientBuilder()
      .codec(Kestrel())
      .hosts(host)
      .hostConnectionLimit(hostConnectionLimit)
      .buildFactory())

  implicit def stringToChannelBuffer(string: String): ChannelBuffer =
    ChannelBuffers.wrappedBuffer(string.getBytes)

  implicit def channelBufferToString(cbuffer: ChannelBuffer): String =
    cbuffer.toString(CharsetUtil.UTF_8)

  implicit def readMessageToString(readmessage: ReadMessage): String =
    channelBufferToString(readmessage.bytes)

  /**
   * Empties all items from the queue without deleting the journal.
   * @param queueName
   * @return
   */
  def flush(queueName: String) = client.flush(queueName)()

  /**
   * Removes the journal file on the remote server.
   * @param queueName
   * @return
   */
  def delete(queueName: String) = client.delete(queueName)()

  /**
   * Enqueue an item.
   * @param queueName
   * @param value
   */
  def set(queueName: String, value: String) {
    set(queueName, value, 0)
  }

  /**
   * Enqueue an item.
   * @param queueName
   * @param value
   * @param expiry how long the item is valid for (Kestrel will delete the item
   *               if it isn't dequeued in time).
   */
  def set(queueName: String, value: String, expiry: Int) {
    client.set(queueName, value, Time.fromMilliseconds(expiry))
  }

  /**
   * Dequeue an item.
   * @param queueName
   * @return
   */
  def get(queueName: String): Option[String] = get(queueName, 0)

  /**
   * Dequeue an item.
   * @param queueName
   * @param waitTime if the queue is empty, indicate to the Kestrel server how
   *                 long to block the operation, waiting for something to arrive, before returning None
   * @return
   */
  def get(queueName: String, waitTime: Int): Option[String] = {
    try {
      val waitUpTo = waitTime.millisecond //Duration(waitTime, TimeUnit.MILLISECONDS)
      client.get(queueName, waitUpTo)() match {
        case Some(value) => Some(value)
        case None => None
      }
    } catch {
      case e: Exception => throw e
    }
  }

  /**
   * Return the first available item from the queue, if there is one, but don't remove it.
   * @param queueName
   * @return
   */
  def peek(queueName: String): Option[String] = peek(queueName, 0)

  /**
   * Return the first available item from the queue, if there is one, but don't remove it.
   * @param queueName
   * @param waitTime if the queue is empty, indicate to the Kestrel server how
   *                 long to block the operation, waiting for something to arrive, before returning None
   * @return
   */
  def peek(queueName: String, waitTime: Int): Option[String] = get(queueName + "/peek", waitTime)

  /**
   * reliable read
   * @param queueName
   * @param retryBackoffs
   * @param duration millisecond
   * @return
   */
  def read(queueName: String, retryBackoffs: Int, duration:Int): List[Option[String]] = {
    val stopped = new AtomicBoolean(false)

    val timer = new JavaTimer(isDaemon = true)
    val readHandle =
      client.readReliably(queueName, timer, Backoff.const(retryBackoffs.milliseconds))

    readHandle.error.foreach {
      e =>
        if (!stopped.get)
          throw e
    }

    var messages = ListBuffer[Option[String]]()
    readHandle.messages.foreach {
      msg =>
        try {
          messages.append(Some(msg))
        } finally {
          msg.ack.sync
        }
    }
    Thread.sleep(duration)
    stopped.set(true)
    readHandle.close

    messages.toList
  }

  /**
   * Close any consume resources such as TCP Connections.
   */
  def close = client.close
}
