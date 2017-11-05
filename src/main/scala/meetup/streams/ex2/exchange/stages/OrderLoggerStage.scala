package meetup.streams.ex2.exchange.stages

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import com.typesafe.scalalogging.StrictLogging
import meetup.streams.ex2.exchange.dal.OrderDao
import meetup.streams.ex2.exchange.om.{Execution, PartialFills}

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

      override def onUpstreamFailure(ex: Throwable): Unit = {
        super.onUpstreamFailure(ex)
        logger.error("Upstream has been failed", ex)
      }
    })
  }
}
