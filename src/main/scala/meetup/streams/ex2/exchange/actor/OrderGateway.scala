package meetup.streams.ex2.exchange.actor

import akka.event.Logging
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import meetup.streams.ex2.exchange.om.Order

import scala.collection.mutable

class OrderGateway extends ActorPublisher[Order] {
  val log = Logging(context.system, this)
  val queue = mutable.Queue[Order]()
  var published = 0

  override def receive = {
    case o: Order =>
      queue.enqueue(o)
      publishIfNeeded()
    case Request(cnt) =>
      publishIfNeeded()
      log.info(s"requested: $cnt")
    case Cancel =>
      log.warning("received Cancel message, going to Complete the Stream!")
      context.stop(self)
    case _ =>
  }

  def publishIfNeeded() = {
    while (queue.nonEmpty && isActive && totalDemand > 0) {
      onNext(queue.dequeue())
      published += 1
      if (published == 100) onComplete()
    }
  }
}
