package com.comnus.kestrel

import org.scalatest.FunSuite
import actors.Actor

class KClientTest extends FunSuite {

  val queueName = "foo"
  val v = "abc"
  val host = "localhost:22133"
  val connection = 2
  val kclient = new KClient(host, connection)

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

  test("block fetch") {
    kclient.flush(queueName)

    Actor.actor {
      Thread.sleep(1000)
      kclient.set(queueName, "def")
    }

    assert(kclient.get(queueName, 5000).get == "def")
  }

  test("read reliably") {
    kclient.flush(queueName)

    kclient.set(queueName, "aaa")
    kclient.set(queueName, "bbb")

    val results = kclient.read(queueName, 10, 2000)

    println("results size: " + results.size)
    assert(results.size == 2)
    assert(results(0).get == "aaa")
    assert(results(1).get == "bbb")
  }

  test("close") {
    kclient.close
    intercept[com.twitter.finagle.ServiceClosedException](
      kclient.get(queueName)
    )
  }

}