
import com.twitter.conversions.time._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.protocol.Kestrel
import com.twitter.finagle.kestrel.{ReadHandle, Client}
import com.twitter.finagle.service.Backoff
import com.twitter.util.{Duration, Time, JavaTimer}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil

object Main{
  def main(args:Array[String]){

    test

    val builder = ClientBuilder().codec(Kestrel()).hosts("localhost:22133").hostConnectionLimit(1)
    val c = builder.buildFactory()
    val client = Client(c)

    val value = ChannelBuffers.wrappedBuffer("aaa".getBytes(CharsetUtil.UTF_8))
    client.set("foo", value, Time.fromMilliseconds(0))

    val waitDuration = Duration(0, TimeUnit.MILLISECONDS)
    val result = client.get("foo", waitDuration)()

    println("result: " + result.get.toString(CharsetUtil.UTF_8))

    client.close

  }

  def test{


    val duration = 10.seconds
    println("running for %s".format(duration))

    // Add "host:port" pairs as needed
    val hosts = Seq("localhost:22133")
    val stopped = new AtomicBoolean(false)

    val clients: Seq[Client] = hosts map { host =>
      Client(ClientBuilder()
        .codec(Kestrel())
        .hosts(host)
        .hostConnectionLimit(1) // process at most 1 item per connection concurrently
        .buildFactory())
    }

    val readHandles: Seq[ReadHandle] = {
      val queueName = "foo"
      val timer = new JavaTimer(isDaemon = true)
      val retryBackoffs = Backoff.const(10.milliseconds)
      clients map { _.readReliably(queueName, timer, retryBackoffs) }
    }

    val readHandle: ReadHandle = ReadHandle.merged(readHandles)

    // Attach an async error handler that prints to stderr
    readHandle.error foreach { e =>
      if (!stopped.get) System.err.println("zomg! got an error " + e)
    }

    // Attach an async message handler that prints the messages to stdout
    readHandle.messages foreach { msg =>
      try {
        println(msg.bytes.toString(CharsetUtil.UTF_8))
      } finally {
        msg.ack.sync() // if we don't do this, no more msgs will come to us
      }
    }

    // Let it run for a little while
    Thread.sleep(duration.inMillis)
    // Without this, we get messages sent to our error handler
    stopped.set(true)

    println("stopping")
    readHandle.close()
    clients foreach { _.close() }
    println("done")
  }

}
