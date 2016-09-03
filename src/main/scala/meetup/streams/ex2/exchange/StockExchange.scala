package meetup.streams.ex2.exchange

import java.math.BigDecimal
import java.time.LocalDateTime

import akka.NotUsed
import akka.actor.{ActorSystem, Props}
import akka.routing.RoundRobinPool
import akka.stream.{ActorMaterializer, ThrottleMode}
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Flow, Sink, Source}
import com.typesafe.scalalogging.StrictLogging
import meetup.streams.ex2.exchange.OrderSource.generateRandomOrder
import meetup.streams.ex2.exchange.actor.{OrderGateway, OrderLogger}
import meetup.streams.ex2.exchange.dal.IOrderDao
import meetup.streams.ex2.exchange.om.{ExecutedQuantity, _}

import scala.collection.immutable.Iterable
import scala.util.Random
import scala.concurrent.duration._

object StockExchange extends App {
  implicit val system = ActorSystem("StockExchange")
  implicit val materializer = ActorMaterializer()

  val orderDao = Config.injector.getInstance(classOf[IOrderDao])
  val orderLogger = RoundRobinPool(nrOfInstances = 1).props(Props(classOf[OrderLogger], orderDao))

  val orderGateway = system.actorOf(Props[OrderGateway])
  val gatewayPublisher = ActorPublisher[Order](orderGateway)

  //OrderSource()
  Source.fromPublisher(gatewayPublisher)
    .via(OrderIdGenerator())
    .via(OrderPersistence(orderDao))
    .via(OrderProcessor())
    .via(OrderExecutor())
    .throttle(1, 1.second, 1, ThrottleMode.shaping)
    .runWith(Sink.actorSubscriber(orderLogger))

  1 to 1000 foreach { _ => orderGateway ! generateRandomOrder }
}

object OrderProcessor {
  def apply(): Flow[LoggedOrder, ExecuteOrder, NotUsed] =
    Flow.fromFunction(o => ExecuteOrder(o.orderId, o.order.quantity))
}

object OrderExecutor extends StrictLogging {
  type PartialFills = Seq[ExecutedQuantity]
  val execQuantity = 3

  def apply(): Flow[ExecuteOrder, PartialFills, NotUsed] = Flow.fromFunction(o => execute(o))

  private def execute(eo: ExecuteOrder): PartialFills = {
    logger.info("Going to execute next order = {}", eo)

    val quantities = Seq.fill(execQuantity)(Random.nextInt(eo.quantity / execQuantity))
    quantities.map { q =>
      ExecutedQuantity(eo.orderId, q, LocalDateTime.now)
    }
  }
}

object OrderPersistence {
  def apply(orderDao: IOrderDao): Flow[PreparedOrder, LoggedOrder, NotUsed] =
    Flow.fromFunction(p => {
      orderDao.saveOrder(new Order(p.orderId, p.order))
      LoggedOrder(p.orderId, p.order)
    })
}

object OrderIdGenerator {
  private var seqNo: Long = 0

  def apply(): Flow[Order, PreparedOrder, NotUsed] = Flow.fromFunction(o => PreparedOrder(o, nextSeqNo()))

  def nextSeqNo(): Long = {
    seqNo += 1
    seqNo
  }
}

object OrderSource extends StrictLogging {
  val symbols = Array("APPL", "GOOG", "IBM", "YAH")

  def generateRandomOrder = new Order(
    OrderType(Random.nextInt(OrderType.values.size)),
    BigDecimal.valueOf(Random.nextDouble * 100),
    symbols(Random.nextInt(symbols.length)),
    Math.abs(Random.nextInt),
    Math.abs(100 + Random.nextInt(500)))


  def apply(): Source[Order, NotUsed] = {
    val orderGenerator: Iterable[Order] = new Iterable[Order] {
      var counter = 0

      override def iterator: Iterator[Order] = new Iterator[Order]() {
        override def hasNext: Boolean = {
          counter += 1
          logger.error("counter = " + counter)
          true
        }

        override def next(): Order = generateRandomOrder
      }
    }

    Source(orderGenerator)
  }
}