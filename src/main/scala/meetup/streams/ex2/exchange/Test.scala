package meetup.streams.ex2.exchange

import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import meetup.streams.ex2.exchange.Common._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Test extends App {
  val source = Source(1 to 100)
  val concat = Flow[Int].filter(_ % 2 == 0)
  //emits result when the upstream completes
  val sink = Sink.fold[Int, Int](0)(_ + _)

  val g: RunnableGraph[Future[Int]] = source.via(concat).toMat(sink)(Keep.right)
  val sum: Future[Int] = g.run
  sum.foreach(print)
}
