package meetup.streams.ex2.exchange.stages

import akka.event.Logging
import akka.stream._
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import com.typesafe.scalalogging.StrictLogging
import meetup.streams.ex2.exchange.OrderSourceStub
import meetup.streams.ex2.exchange.om.Order

import scala.collection.mutable

class OrderGateway extends ActorPublisher[Order] {
  val log = Logging(context.system, this)
  private val queue = mutable.Queue[Order]()
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

  private def publishIfNeeded(): Unit = {
    while (queue.nonEmpty && isActive && totalDemand > 0) {
      val order = queue.dequeue
      log.debug(s"publish new order to the stream $order")
      onNext(order)
      published += 1
      if (published == 100) onComplete()
    }
  }
}

class RandomOrderSource(limit: Int) extends GraphStage[SourceShape[Order]] with StrictLogging {
  private val out = Outlet[Order]("RandomOrderSource.out")
  val shape = SourceShape.of(out)

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
    var count = 0

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (count < limit) {
          push(out, OrderSourceStub.generateRandomOrder)
          count += 1
        } else {
          complete(out)
          logger.info("Completing order source...")
        }
      }
    })
  }
}