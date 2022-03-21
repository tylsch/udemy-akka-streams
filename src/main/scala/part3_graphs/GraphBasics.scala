package part3_graphs

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ClosedShape
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source, Zip}

import scala.concurrent.duration._
import scala.language.postfixOps

object GraphBasics extends App {

  implicit val system: ActorSystem = ActorSystem("GraphBasics")

  val input = Source(1 to 1000)
  val incrementer = Flow[Int].map(_ + 1)
  val multiplier = Flow[Int].map(_ * 10)
  val output = Sink.foreach[(Int, Int)](println)

  val graph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] => // builder = MUTABLE data structure
      import GraphDSL.Implicits._ // brings some nice operators into scope

      // Step 2 - add the necessary components of this graph
      val broadcast = builder.add(Broadcast[Int](2)) // fan-out operator
      val zip = builder.add(Zip[Int, Int]) // fan-in operator

      // Step 3 - tying up the components
      input ~> broadcast

      broadcast.out(0) ~> incrementer ~> zip.in0
      broadcast.out(1) ~> multiplier ~> zip.in1

      zip.out ~> output

      // Step 4 - return a ClosedShape
      ClosedShape // FREEZE the builder's shape
      //shape object
    }// graph
  ) //runnable graph

//  graph.run()

  val firstSink = Sink.foreach[Int](x => println(s"First Sink: $x"))
  val secondSink = Sink.foreach[Int](x => println(s"Second Sink: $x"))

  val twoSinksGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] => // builder = MUTABLE data structure
      import GraphDSL.Implicits._ // brings some nice operators into scope

      // Step 2 - add the necessary components of this graph
      val broadcast = builder.add(Broadcast[Int](2))

      // Step 3 - tying up the components
      input ~> broadcast ~> firstSink //implicit port numbering
               broadcast ~> secondSink

//      broadcast.out(0) ~> firstSink
//      broadcast.out(1) ~> secondSink
      // Step 4 - return a ClosedShape
      ClosedShape // FREEZE the builder's shape
      //shape object
    }// graph
  ) //runnable graph

//  twoSinksGraph.run()

  val fastSource = input.throttle(5, 1 second)
  val slowSource = input.throttle(2, 1 second)

  val sink1 = Sink.fold[Int, Int](0)((count, _) => {
    println(s"Sink 1 number of elements: $count")
    count + 1
  })
  val sink2 = Sink.fold[Int, Int](0)((count, _) => {
    println(s"Sink 2 number of elements: $count")
    count + 1
  })

  val balanceGraph = RunnableGraph.fromGraph(
    GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] => // builder = MUTABLE data structure
      import GraphDSL.Implicits._ // brings some nice operators into scope

      // Step 2 - add the necessary components of this graph
      val merge = builder.add(Merge[Int](2))
      val balance = builder.add(Balance[Int](2))

      // Step 3 - tying up the components
      fastSource ~> merge ~> balance ~> sink1
      slowSource ~> merge;   balance ~> sink2

      // Step 4 - return a ClosedShape
      ClosedShape // FREEZE the builder's shape
      //shape object
    }// graph
  ) //runnable graph

  balanceGraph.run()
}
