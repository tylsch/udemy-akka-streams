package part4_techniques

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}

class TestingStreamsSpec extends TestKit(ActorSystem("TestingAkkaStreams"))
  with ImplicitSender
  with AnyWordSpecLike
  with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A simple stream" should {
    "satisfy basic assertions" in {
      val simpleSource = Source(1 to 10)
      val simpleSink = Sink.fold(0)((a: Int, b: Int) => a + b)

      val sumFuture = simpleSource.toMat(simpleSink)(Keep.right).run()
      val sum = Await.result(sumFuture, 2 seconds)
      assert(sum == 55)
    }

    "integrate with test actors via materialized actors" in {
      import akka.pattern.pipe
      import system.dispatcher
      val simpleSource = Source(1 to 10)
      val simpleSink = Sink.fold(0)((a: Int, b: Int) => a + b)

      val probe = TestProbe()

      simpleSource.toMat(simpleSink)(Keep.right).run().pipeTo(probe.ref)

      probe.expectMsg(55)
    }

    "integrate with Streams Testkit Sink" in {
      val sourceUnderTest = Source(1 to 5).map(_ * 2)
      val testSink = TestSink.probe[Int]
      val materializedTestValue = sourceUnderTest.runWith(testSink)

      materializedTestValue
        .request(5)
        .expectNext(2, 4, 6, 8, 10)
        .expectComplete()
    }

    "integrate with Streams Testkit Source" in {
      import system.dispatcher
      val sinkUnderTest = Sink.foreach[Int] {
        case 13 => throw new RuntimeException("back luck")
        case _ =>
      }

      val testSource = TestSource.probe[Int]
      val materialized = testSource.toMat(sinkUnderTest)(Keep.both).run()
      val (testPublisher, resultFuture) = materialized

      testPublisher
        .sendNext(1)
        .sendNext(5)
        .sendNext(13)
        .sendComplete()

      resultFuture.onComplete {
        case Success(_) => fail("the sink under test should have thrown an exception")
        case Failure(_) =>
      }
    }

    "test flows with a test source and test sink" in {
      val flowUnderTest = Flow[Int].map(_ * 2)

      val testSource = TestSource.probe[Int]
      val testSink = TestSink.probe[Int]

      val materializedValue = testSource.via(flowUnderTest).toMat(testSink)(Keep.both).run()
      val (publisher, subscriber) = materializedValue

      publisher
        .sendNext(1)
        .sendNext(5)
        .sendNext(42)
        .sendNext(99)
        .sendComplete()

      subscriber
        .request(4)
        .expectNext(2, 10, 84, 198)
        .expectComplete()
    }
  }

}
