package part5_advance

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}

import scala.util.{Failure, Success}

object Substreams extends App {

  implicit val system: ActorSystem = ActorSystem("Substreams")
  import system.dispatcher

  // 1 - Grouping a stream by a certain function
  val wordSource = Source(List("Akka", "is", "amazing", "learning", "sub-streams"))
  val groups = wordSource.groupBy(30, word => if (word.isEmpty) "\\u0000" else word.toLowerCase().charAt(0))

  groups.to(Sink.fold(0)((count, word) => {
    val newCount = count + 1
    println(s"I just received $word, count is $newCount")
    newCount
  })).run()

  // 2 - Merge substreams back
  val textSource = Source(List(
    "I love akka streams",
    "this is amazing",
    "learning from rock the jvm"
  ))

  val totalCharacterCount = textSource.groupBy(2, s => s.length % 2)
    .map(_.length) // do your expensive computation here
    .mergeSubstreamsWithParallelism(2)
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
    .run()

  totalCharacterCount.onComplete {
    case Success(value) => println(s"Total char count: $value")
    case Failure(exception) => println(s"Char computation exception: $exception")
  }

  // 3 - Splitting a stream into substreams when a condition is met
  val text =
    "I love akka streams\n" +
      "this is amazing\n" +
      "learning from rock the jvm\n"

  val anotherCharCountFuture = Source(text.toList)
    .splitWhen(c => c == '\n')
    .filter(_ != '\n')
    .map(_ => 1)
    .mergeSubstreams
    .toMat(Sink.reduce[Int](_ + _))(Keep.right)
    .run()

  anotherCharCountFuture.onComplete {
    case Success(value) => println(s"Total char count alternative: $value")
    case Failure(exception) => println(s"Char computation exception: $exception")
  }

  // 4 - Flattening
  val simpleSource = Source(1 to 5)
  simpleSource.flatMapConcat(x => Source(x to x * 3)).runWith(Sink.foreach(println))
  simpleSource.flatMapMerge(2, x => Source(x to x * 3)).runWith(Sink.foreach(println))
}
