package com.yysy.kestrel

import org.scalatest.FunSuite
import actors.Actor
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.protocol._
import com.twitter.util.Time
import org.jboss.netty.buffer.ChannelBuffers

class KClientTest extends FunSuite {

  val queueName = "foo"
  val v = "abc"
  val kclient = new KClient("localhost:22133")

  implicit def stringToChannelBuffer(string: String) =
    ChannelBuffers.wrappedBuffer(string.getBytes)

  test("Set and Get") {
    kclient.flush(queueName)
    kclient.set(queueName, v)
    val value = kclient.get(queueName).get

    println("value: " + value)
    assert(value == v)
  }

  test("empty get") {
    kclient.flush(queueName)
    assert(kclient.get(queueName) == None)
  }

  test("get from non-existent queue") {
    kclient.delete("abc")
    assert(kclient.get("abc") == None)
  }

  test("peek") {
    kclient.flush(queueName)
    kclient.set(queueName, "abc")

    kclient.peek(queueName)
    val result = kclient.get(queueName).get
    assert(result == "abc")
    assert(kclient.get(queueName) == None)
  }

  /*
  test("wait") {
    Actor.actor{
      Thread.sleep(1000)
      kclient.set(queueName, "def")
    }

    println(kclient.get(queueName, 3000))
  }
  */

}