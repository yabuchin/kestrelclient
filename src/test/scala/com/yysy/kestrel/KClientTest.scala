package com.yysy.kestrel

import org.scalatest.FunSuite

class KClientTest extends FunSuite {

  val queueName = "foo"
  val v = "abc"
  val kclient = new KClient("localhost:22133")

  test("Set and Get") {
    kclient.set(queueName, v)
    val value = kclient.get(queueName)

    println("value: " + value)
    assert(value == v)
  }

  test("empty get") {
    intercept[NoSuchElementException] {
      kclient.get(queueName)
    }
  }

  test("get from non-existent queue") {
    intercept[NoSuchElementException] {
      kclient.get("abc")
    }
  }

  test("reliable read") {
    kclient.set(queueName, "abc")
    kclient.set(queueName, "def")
    kclient.set(queueName, "ggg")

    kclient.read(queueName)
  }

}