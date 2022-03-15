package part2_primer

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, RunnableGraph, Sink, Source}

import scala.concurrent.Future

object FirstPrinciples extends App {

  implicit val system: ActorSystem = ActorSystem("FirstPrinciples")

  // sources
  val source: Source[Int, NotUsed] = Source(1 to 10)
  // sinks
  val sink: Sink[Int, Future[Done]] = Sink.foreach[Int](println)

  val graph: RunnableGraph[NotUsed] = source.to(sink)
//  graph.run()

  // flows = transform elements
  val flow = Flow[Int].map(x => x + 1)
  val sourceWithFlow: Source[Int, NotUsed] = source.via(flow)
  val flowWithSink: Sink[Int, NotUsed] = flow.to(sink)

//  sourceWithFlow.to(sink).run()
//  source.to(flowWithSink).run()
//  source.via(flow).to(sink).run()

  // nulls are NOT allowed
//  val illegalSource = Source.single[String](null)
//  illegalSource.to(Sink.foreach(println))
  // use Options instead

  // various kinds of sources
  val finiteSource = Source.single(1)
  val anotherFiniteSource = Source(List(1,2,3,4))
  val emptySource = Source.empty[Int]
//  val infiniteSource = Source(Stream.from(1))

  import scala.concurrent.ExecutionContext.Implicits.global
  val futureSource = Source.future(Future(42))

  // sinks
  val theMostBoringSink = Sink.ignore
  val foreachSink = Sink.foreach[String](println)
  val headSink = Sink.head[Int] // retrieves head and then closes the stream
  val foldSink = Sink.fold[Int, Int](0)((a,b) => a + b)

  // flows = usually mapped to collection operators
  val mapFlow = Flow[Int].map(x => x * 2)
  val takeFlow = Flow[Int].take(5)
  // drop, filter, etc...
  // DO NOT have flatMap (or flatMap like operators)

  // source -> flow -> flow -> flow(n) -> sink
  val doubleFlowGraph = source.via(mapFlow).via(takeFlow).to(sink)
//  doubleFlowGraph.run()

  // syntactic sugars
  val mapSource = Source(1 to 10).map(x => x * 2)
  // run streams directly
//  mapSource.runForeach(println) // mapSource.to(Sink.foreach[Int](println)).run()

  // OPERATORS = components

  val personGraph = Source(List("Bob", "Jim", "Carrie", "AkkaStreams")).filter(x => x.length > 5)
  personGraph.runForeach(x => println(s"Person name $x"))
}
