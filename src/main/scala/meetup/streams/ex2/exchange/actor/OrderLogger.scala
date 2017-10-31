package meetup.streams.ex2.exchange.actor

import akka.event.Logging
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.typesafe.scalalogging.StrictLogging
import meetup.streams.ex2.exchange.dal.OrderDao
import meetup.streams.ex2.exchange.om.{ExecutedQuantity, Execution, PartialFills}

import scala.collection.mutable.ListBuffer

class OrderLogger(orderDao: OrderDao) extends ActorSubscriber {
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

class OrderLoggerStage(orderDao: OrderDao) extends GraphStage[SinkShape[PartialFills]] with StrictLogging {
  private val in = Inlet[PartialFills]("OrderLoggerStage.in")
  val shape: SinkShape[PartialFills] = SinkShape.of(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    override def preStart(): Unit = {
      // a detached stage needs to start upstream demand
      // itself as it is not triggered by downstream demand
      pull(in)
    }

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val executions = grab(in).seq
        logger.info(s"logging executions to db: ${executions.length}")

        executions.foreach { e =>
          orderDao.insertExecution(Execution(e.orderId, e.quantity, e.executionDate))
          logger.info("saved execution = {}", e)
        }
        pull(in)
      }
    })
  }
}
