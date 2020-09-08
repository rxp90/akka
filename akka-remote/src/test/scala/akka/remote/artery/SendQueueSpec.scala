/*
 * Copyright (C) 2016-2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.remote.artery

import java.util.Queue

import akka.actor.Actor
import akka.actor.Props
import akka.stream.ActorMaterializer
import akka.stream.ActorMaterializerSettings
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.AkkaSpec
import akka.testkit.ImplicitSender
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue

import akka.stream.impl.FastDroppingQueue

object SendQueueSpec {

  case class ProduceToQueue(from: Int, until: Int, queue: Queue[Msg])
  case class ProduceToQueueValue(from: Int, until: Int, queue: FastDroppingQueue[Msg])
  case class Msg(fromProducer: String, value: Int)

  def producerProps(producerId: String): Props =
    Props(new Producer(producerId))

  class Producer(producerId: String) extends Actor {
    def receive = {
      case ProduceToQueue(from, until, queue) =>
        var i = from
        while (i < until) {
          if (!queue.offer(Msg(producerId, i)))
            throw new IllegalStateException(s"offer failed from $producerId value $i")
          i += 1
        }
      case ProduceToQueueValue(from, until, queue) =>
        var i = from
        while (i < until) {
          if (queue.offer(Msg(producerId, i)) != FastDroppingQueue.OfferResult.Enqueued)
            throw new IllegalStateException(s"offer failed from $producerId value $i")
          i += 1
        }
    }

  }
}

class SendQueueSpec extends AkkaSpec("akka.actor.serialize-messages = off") with ImplicitSender {
  import SendQueueSpec._

  val matSettings = ActorMaterializerSettings(system).withFuzzing(true)
  implicit val mat = ActorMaterializer(matSettings)(system)

  def sendToDeadLetters[T](pending: Vector[T]): Unit =
    pending.foreach(system.deadLetters ! _)

  def createQueue[E](capacity: Int): Queue[E] = {
    // new java.util.concurrent.LinkedBlockingQueue[E](capacity)
    new ManyToOneConcurrentArrayQueue[E](capacity)
  }

  "SendQueue" must {

    "deliver all messages" in {
      val (sendQueue, downstream) =
        Source.fromGraph(FastDroppingQueue[String](128)).toMat(TestSink.probe)(Keep.both).run()

      downstream.request(10)
      sendQueue.offer("a")
      sendQueue.offer("b")
      sendQueue.offer("c")
      downstream.expectNext("a")
      downstream.expectNext("b")
      downstream.expectNext("c")
      downstream.cancel()
    }

//    "deliver messages enqueued before materialization" in {
//      queue.offer("a")
//      queue.offer("b")
//
//      val (sendQueue, downstream) =
//        Source.fromGraph(FastDroppingQueue[String](128)).toMat(TestSink.probe)(Keep.both).run()
//
//      downstream.request(10)
//      downstream.expectNoMessage(200.millis)
//      downstream.expectNext("a")
//      downstream.expectNext("b")
//
//      sendQueue.offer("c")
//      downstream.expectNext("c")
//      downstream.cancel()
//    }

    "deliver bursts of messages" in {
      // this test verifies that the wakeup signal is triggered correctly
      val burstSize = 100
      val (sendQueue, downstream) =
        Source.fromGraph(FastDroppingQueue[Int](128)).grouped(burstSize).async.toMat(TestSink.probe)(Keep.both).run()

      downstream.request(10)

      for (round <- 1 to 100000) {
        for (n <- 1 to burstSize) {
          if (sendQueue.offer(round * 1000 + n) != FastDroppingQueue.OfferResult.Enqueued)
            fail(s"offer failed at round $round message $n")
        }
        downstream.expectNext((1 to burstSize).map(_ + round * 1000).toList)
        downstream.request(1)
      }

      downstream.cancel()
    }

    "support multiple producers" in {
      val numberOfProducers = 5
      val producers = Vector.tabulate(numberOfProducers)(i => system.actorOf(producerProps(s"producer-$i")))

      val (sendQueue, downstream) =
        Source.fromGraph(FastDroppingQueue[Msg](numberOfProducers * 512)).toMat(TestSink.probe)(Keep.both).run()

      // send 100 per producer
      producers.foreach(_ ! ProduceToQueueValue(0, 100, sendQueue))

      producers.foreach(_ ! ProduceToQueueValue(100, 200, sendQueue))

      // send 100 more per producer
      downstream.request(producers.size * 200)
      val msgByProducer = downstream.expectNextN(producers.size * 200).groupBy(_.fromProducer)
      (0 until producers.size).foreach { i =>
        msgByProducer(s"producer-$i").map(_.value) should ===(0 until 200)
      }

      // send 500 per producer
      downstream.request(producers.size * 1000) // more than enough
      producers.foreach(_ ! ProduceToQueueValue(200, 700, sendQueue))
      val msgByProducer2 = downstream.expectNextN(producers.size * 500).groupBy(_.fromProducer)
      (0 until producers.size).foreach { i =>
        msgByProducer2(s"producer-$i").map(_.value) should ===(200 until 700)
      }

      downstream.cancel()
    }

    "deliver first message" ignore {

      def test(f: (Queue[String], SendQueue.QueueValue[String], TestSubscriber.Probe[String]) => Unit): Unit = {

        (1 to 100).foreach { _ =>
          val queue = createQueue[String](16)
          val (sendQueue, downstream) =
            Source.fromGraph(new SendQueue[String](sendToDeadLetters)).toMat(TestSink.probe)(Keep.both).run()

          f(queue, sendQueue, downstream)
          downstream.expectNext("a")

          sendQueue.offer("b")
          downstream.expectNext("b")
          sendQueue.offer("c")
          sendQueue.offer("d")
          downstream.expectNext("c")
          downstream.expectNext("d")
          downstream.cancel()
        }
      }

      test { (queue, sendQueue, downstream) =>
        queue.offer("a")
        downstream.request(10)
        sendQueue.inject(queue)
      }
      test { (queue, sendQueue, downstream) =>
        sendQueue.inject(queue)
        queue.offer("a")
        downstream.request(10)
      }

      test { (queue, sendQueue, downstream) =>
        queue.offer("a")
        sendQueue.inject(queue)
        downstream.request(10)
      }
      test { (queue, sendQueue, downstream) =>
        downstream.request(10)
        queue.offer("a")
        sendQueue.inject(queue)
      }

      test { (queue, sendQueue, downstream) =>
        sendQueue.inject(queue)
        downstream.request(10)
        sendQueue.offer("a")
      }
      test { (queue, sendQueue, downstream) =>
        downstream.request(10)
        sendQueue.inject(queue)
        sendQueue.offer("a")
      }
      test { (queue, sendQueue, downstream) =>
        sendQueue.inject(queue)
        sendQueue.offer("a")
        downstream.request(10)
      }
    }

  }

}
