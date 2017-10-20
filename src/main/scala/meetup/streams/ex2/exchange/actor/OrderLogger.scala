package meetup.streams.ex2.exchange.actor

import akka.event.Logging
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import meetup.streams.ex2.exchange.dal.IOrderDao
import meetup.streams.ex2.exchange.om.{ExecutedQuantity, Execution, PartialFills}

import scala.collection.mutable.ListBuffer

class OrderLogger(orderDao: IOrderDao) extends ActorSubscriber {
  val log = Logging(context.system, this)
  private val queue = ListBuffer[ExecutedQuantity]()

  override protected def requestStrategy: RequestStrategy = new WatermarkRequestStrategy(highWatermark = 10, lowWatermark = 5)

  override def receive = {
    case OnNext(e: PartialFills) =>
      queue ++= e.seq
      log.warning("queue size = {}", queue.length)
      e.seq.foreach(q => self ! q)
    case eq: ExecutedQuantity =>
      orderDao.insertExecution(Execution(eq.orderId, eq.quantity, eq.executionDate))
      queue -= eq
      log.warning("queue size after = {}", queue.length)
      log.info("saved execution = {}", eq)
    case OnError(e) => log.error(e, "Received error from stream")
    case OnComplete => log.info("Stream has been completed >>>>>>>>>>>>>>>>>>>>>")
  }
}
