package part4_techniques

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.pattern.ask
import akka.stream.scaladsl.{Sink, Source}
import akka.util.Timeout

import java.util.Date
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

object IntegratingWithExternalServices extends App {

  implicit val system: ActorSystem = ActorSystem("IntegratingWithExternalServices")
  import system.dispatcher // not recommended for practice for mapAsync
  implicit val dispatcher = system.dispatchers.lookup("dedicated-dispatcher")

  def genericExternalService[A](element: A): Future[A] = ???

  // Example Simplified PagerDuty
  case class PagerEvent(application: String, description: String, date: Date)

  val eventSource = Source(List(
    PagerEvent("AkkaInfra", "Infrastructure broke", new Date),
    PagerEvent("FastDataPipeline", "Illegal elements in the data pipeline", new Date),
    PagerEvent("AkkaInfra", "A service stopped responding", new Date),
    PagerEvent("SuperFrontEnd", "A button doesn't work", new Date)
  ))

  class PagerActor extends Actor with ActorLogging {
    private val engineers = List("Daniel", "John", "Steve")
    private val emails = Map(
      "Daniel" -> "daniel@gmail.com",
      "John" -> "john@gmail.com",
      "Steve" -> "steve@gmail.com"
    )

    private def processEvent(pagerEvent: PagerEvent) = {
      val engineerIndex = pagerEvent.date.toInstant.getEpochSecond / (24 * 3680) % engineers.length
      val engineer = engineers(engineerIndex.toInt)
      val engineerEmail = emails(engineer)

      // page the engineer
      log.info(s"Sending engineer $engineerEmail a high priority notification: $pagerEvent")
      Thread.sleep(1000)

      engineerEmail
    }

    override def receive: Receive = {
      case pagerEvent: PagerEvent =>
        sender() ! processEvent(pagerEvent)
    }
  }

  val infraEvents = eventSource.filter(_.application == "AkkaInfra")
//  val pagedEngineerEmails = infraEvents.mapAsync(parallelism = 4)(event => PagerService.processEvent(event))
  // guarantees the relative order of elements
  val pagedEmailsSink = Sink.foreach[String](email => println(s"Successfully sent notification to $email"))

  //pagedEngineerEmails.to(pagedEmailsSink).run()

  implicit val timeout: Timeout = Timeout(10 second)
  val pagerActor = system.actorOf(Props[PagerActor], "pagerActor")
  val alternativePagedEngineerEmails = infraEvents.mapAsync(parallelism = 4)(event => (pagerActor ? event).mapTo[String])
  alternativePagedEngineerEmails.to(pagedEmailsSink).run()

  // do not confuse mapAsync with async (ASYNC boundary)
}
