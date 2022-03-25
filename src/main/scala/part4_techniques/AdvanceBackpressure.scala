package part4_techniques

import akka.actor.ActorSystem
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.{Flow, Sink, Source}

import java.util.Date
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

object AdvanceBackpressure extends App {
  implicit val system: ActorSystem = ActorSystem("AdvanceBackpressure")

  // control backpressure
  val controlledFlow = Flow[Int].map(_ * 2).buffer(10, OverflowStrategy.dropHead)

  case class PagerEvent(description: String, date: Date, nInstances: Int = 1)
  case class Notification(email: String, pagerEvent: PagerEvent)

  val events = List(
    PagerEvent("Service discovery failed", new Date),
    PagerEvent("Illegal elements in the data pipeline", new Date),
    PagerEvent("Number of HTTP 500 spiked", new Date),
    PagerEvent("A service stopped responding", new Date)
  )
  val eventSource = Source(events)

  val onCallEngineer = "tylsch@gmail.com"

  def sendEmail(notification: Notification) = println(s"Dear ${notification.email}, you have an event: ${notification.pagerEvent}")

  val notificationSink = Flow[PagerEvent].map(event => Notification(onCallEngineer, event)).to(Sink.foreach[Notification](sendEmail))

  //standard
//  eventSource.to(notificationSink).run()

  // non-backpressure source
  def sendEmailSlow(notification: Notification) = {
    Thread.sleep(1000)
    println(s"Dear ${notification.email}, you have an event: ${notification.pagerEvent}")
  }

  val aggregateNotificationFlow = Flow[PagerEvent]
    .conflate((event1, event2) => {
      val nInstances = event1.nInstances + event2.nInstances
      PagerEvent(s"You have $nInstances events that require your attention", new Date, nInstances)
    })
    .map(event => Notification(onCallEngineer, event))

//  eventSource.via(aggregateNotificationFlow).async.to(Sink.foreach[Notification](sendEmailSlow)).run()
  // alternative to backpressure

  /*
  * Slow producers: extrapolate/expand
  * */
  val slowCounter = Source(LazyList.from(1)).throttle(1, 1 second)
  val hungrySink = Sink.foreach[Int](println)

  val extrapolator = Flow[Int].extrapolate(element => Iterator.from(element))
  val repeater = Flow[Int].extrapolate(element => Iterator.continually(element))

  slowCounter.via(extrapolator).to(hungrySink).run()

  val expander = Flow[Int].expand(element => Iterator.from(element))

}