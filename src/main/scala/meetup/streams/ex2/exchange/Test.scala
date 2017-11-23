package meetup.streams.ex2.exchange

import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import meetup.streams.ex2.exchange.Common._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

object Test extends App {
  val source = Source(1 to 100)
  val filter = Flow[Int].filter(_ % 2 == 0)
  //emits result when the upstream completes
  val sink = Sink.fold[Int, Int](0)(_ + _)

  val g: RunnableGraph[Future[Int]] = source.via(filter).toMat(sink)(Keep.right)
  val sum: Future[Int] = g.run
  sum.foreach(print)


  // An example from Colin Breck article:

  val random = new Random

  case class Status(text: String = "Hi I am status message")

  case class Sample(currentTime: Long, nextFloat: Float)

  val status =
    Source.tick(0 minute, 5 seconds, ())
      .map(_ => Status)

  Source.tick(0 milliseconds, 1 second, ())
    .map(_ => Sample(System.currentTimeMillis(), random.nextFloat()))
    .merge(status)
    .runWith(Sink.foreach(println))
}
