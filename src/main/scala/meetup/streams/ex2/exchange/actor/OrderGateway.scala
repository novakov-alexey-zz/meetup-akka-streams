package meetup.streams.ex2.exchange.actor

import akka.event.Logging
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import meetup.streams.ex2.exchange.om.Order

import scala.collection.mutable

class OrderGateway extends ActorPublisher[Order] {
  val log = Logging(context.system, this)
  var queue = mutable.Queue[Order]()

  override def receive = {
    case o: Order =>
      queue.enqueue(o)
      publishIfNeeded()
    case Request(cnt) =>
      publishIfNeeded()
      log.info(s"requested $cnt")
    case Cancel => context.stop(self)
    case _ =>
  }

  def publishIfNeeded() = {
    while (queue.nonEmpty && isActive && totalDemand > 0) {
      onNext(queue.dequeue())
    }
  }
}
