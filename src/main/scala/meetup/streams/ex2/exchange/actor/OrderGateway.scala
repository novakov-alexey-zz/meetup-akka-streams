package meetup.streams.ex2.exchange.actor

import akka.event.Logging
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import com.typesafe.scalalogging.StrictLogging
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
      onNext(queue.dequeue())
      published += 1
      if (published == 100) onComplete()
    }
  }
}

class OrderGatewayShape extends GraphStage[FlowShape[Order, Order]] with StrictLogging {
  val in = Inlet[Order]("OrderGatewayShape.in")
  val out = Outlet[Order]("OrderGatewayShape.out")
  val shape = FlowShape.of(in, out)
  val queueSize = 100

  override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
    var published = 0
    val queue = mutable.Queue[Order]()

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        queue.enqueue(grab(in))

        if (queue.length == queueSize) {
          logger.info(s"Flushing internal message queue of $queueSize messages")
          queue.dequeueAll(_ => true).foreach(o => push(out, o))
        }
        else pull(in)
      }
    })
    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        pull(in)
      }
    })
  }
}