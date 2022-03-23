package part4_techniques

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.{CompletionStrategy, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout

import scala.concurrent.duration._
import scala.language.postfixOps

object IntegratingWithActors extends App {
  implicit val system: ActorSystem = ActorSystem("IntegratingWithActors")

  class SimpleActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case s: String =>
        log.info(s"Just received a string: $s")
        sender() ! s"$s$s"
      case n: Int =>
        log.info(s"Just received a number: $n")
        sender() ! (2 * n)
      case _ =>
    }
  }

  val simpleActor = system.actorOf(Props[SimpleActor], "simpleActor")
  val numberSource = Source(1 to 10)

  // actor as a flow
  implicit val timeout: Timeout = Timeout(2 second)
  val actorBasedFlow = Flow[Int].ask[Int](parallelism = 4)(simpleActor)

//  numberSource.via(actorBasedFlow).to(Sink.ignore).run()
//  numberSource.ask[Int](parallelism = 4)(simpleActor).to(Sink.ignore).run() // same as previous line

  /*
  * Actor as a source
  * */
  val actorPoweredSource = Source.actorRef(
    completionMatcher = {
      case Done =>
        // complete stream immediately if we send it Done
        CompletionStrategy.immediately
    },
    // never fail the stream because of a message
    failureMatcher = PartialFunction.empty,
    bufferSize = 10,
    overflowStrategy = OverflowStrategy.dropHead)

  val materializedActorRef = actorPoweredSource.to(Sink.foreach[Int](n => println(s"Actor powered flow got number: $n"))).run()
  materializedActorRef ! 10
  // terminating the stream
  materializedActorRef ! akka.actor.Status.Success("complete")

  /*
  * Actor as a destination/sink
  * - an init message
  * - an ack message to confirm the reception
  * - a complete message
  * - a function to generate a message inn case the stream throws an exception
  * */

  case object StreamInit
  case object StreamAck
  case object StreamComplete
  case class StreamFail(ex: Throwable)

  class DestinationActor extends Actor with ActorLogging {
    override def receive: Receive = {
      case StreamInit =>
        log.info("Stream Initialized")
        sender() ! StreamAck
      case StreamComplete =>
        log.info("Stream Complete")
        context.stop(self)
      case StreamFail(ex) =>
        log.error(s"Stream failed: $ex")
      case message =>
        log.info(s"Message $message has come to its final resting point")
        sender() ! StreamAck
    }
  }
  val destinationActor = system.actorOf(Props[DestinationActor], "destinationActor")

  val actorPoweredSink = Sink.actorRefWithBackpressure(
    destinationActor,
    onInitMessage = StreamInit,
    onCompleteMessage = StreamComplete,
    ackMessage = StreamAck,
    onFailureMessage = throwable => StreamFail(throwable)
  )

  Source(1 to 10).to(actorPoweredSink).run()
}
