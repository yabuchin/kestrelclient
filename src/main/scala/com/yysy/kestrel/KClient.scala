package com.yysy.kestrel

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Client
import com.twitter.util.{Duration, Time}
import com.twitter.finagle.kestrel.protocol._

import java.util.concurrent.TimeUnit

import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.buffer.ChannelBuffers

/**
 * host is pair hostname with port (e.g. localhost:22133 )
 * @param host
 */
class KClient(host: String) {

  private val client =
    Client(ClientBuilder()
      .codec(Kestrel())
      .hosts(host)
      .hostConnectionLimit(1)
      .buildFactory())

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
  def set(queueName: String, value: String) {set(queueName, value, 0)}

  /**
   * Enqueue an item.
   * @param queueName
   * @param value
   * @param expiry how long the item is valid for (Kestrel will delete the item
   * if it isn't dequeued in time).
   */
  def set(queueName: String, value: String, expiry: Int) {
    val v = ChannelBuffers.wrappedBuffer(value.getBytes)
    client.set(queueName, v, Time.fromMilliseconds(expiry))
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
   * long to block the operation, waiting for something to arrive, before returning None
   * @return
   */
  def get(queueName: String, waitTime: Int): Option[String] = {
    try{
      val waitUpTo = Duration(waitTime, TimeUnit.MILLISECONDS)
      client.get(queueName, waitUpTo)() match {
        case Some(value) => Some(value.toString(CharsetUtil.UTF_8))
        case None => None
      }
    }catch{
      case e:Exception => throw e
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
   * long to block the operation, waiting for something to arrive, before returning None
   * @return
   */
  def peek(queueName: String, waitTime: Int): Option[String] = get(queueName + "/peek", waitTime)

  /**
   * Close any consume resources such as TCP Connections.
   */
  def close {client.close}
}
