package com.yysy.kestrel

import org.scalatest.FunSuite

class KClientTest extends FunSuite {

  val queuename = "foo"
  val v = "abc"
  val kclient = new KClient("localhost:22133")

  test("Set and Get") {
    kclient.set(queuename, v)
    val value = kclient.get(queuename)

    println("value: " + value)
    assert(value == v)
  }

  test("empty get") {
    intercept[NoSuchElementException] {
      kclient.get(queuename)
    }
  }

  test("get from non-existent queue") {
    intercept[NoSuchElementException] {
      kclient.get("abc")
    }
  }

}