package meetup.streams.ex2.exchange

import java.math.BigDecimal
import java.time.LocalDateTime

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.actor.ActorPublisher
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape}
import com.typesafe.scalalogging.StrictLogging
import meetup.streams.ex2.exchange.Common._
import meetup.streams.ex2.exchange.OrderSourceStub.generateRandomOrder
import meetup.streams.ex2.exchange.actor.{OrderGateway, OrderLogger}
import meetup.streams.ex2.exchange.dal.IOrderDao
import meetup.streams.ex2.exchange.om.{ExecutedQuantity, _}
import org.reactivestreams.Publisher

import scala.collection.immutable.Iterable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

/*
Todo for Slides: finite and infinite streams
 */
object Common {
  implicit val system: ActorSystem = ActorSystem("StockExchange")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val orderDao: IOrderDao = Config.injector.getInstance(classOf[IOrderDao])
  val orderLogger = Props(classOf[OrderLogger], orderDao)

  val orderGateway: ActorRef = system.actorOf(Props[OrderGateway])
  val gatewayPublisher: Publisher[Order] = ActorPublisher[Order](orderGateway)
}

object StockExchangeGraphPool extends App {
  val workers = 5

  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
    import akka.stream.scaladsl.GraphDSL.Implicits._
    val bcast = b.add(Balance[PartialFills](workers))

    Source.fromPublisher(gatewayPublisher)
      .via(OrderIdGenerator())
      .via(OrderPersistence(orderDao))
      .via(OrderProcessor())
      .via(OrderExecutor()) ~> bcast.in

    for (i <- 0 until workers)
      bcast.out(i) ~> Sink.actorSubscriber(orderLogger).named(s"ol-$i")

    ClosedShape // will throw an exception if it is not really closed graph
  })
  g.run()

  1 to 1000 foreach { _ => orderGateway ! generateRandomOrder }
}

object StockExchangeGraphMat extends App {
  val source = Source.fromPublisher(gatewayPublisher)
    .via(OrderIdGenerator())
    .via(OrderPersistence(orderDao))
    .via(OrderProcessor())
    .via(OrderExecutor())

  val concatFlow = Flow[PartialFills].mapConcat(_.seq.toList)
  val printDate = Sink.fold[String, ExecutedQuantity]("")((s, eq) => s + eq.executionDate + " ")
  val logOrder = Sink.actorSubscriber(orderLogger)

  val (dates, actor) = RunnableGraph.fromGraph(GraphDSL.create(printDate, logOrder)((_, _)) { implicit builder =>
    (pDate, lOrder) =>
      import akka.stream.scaladsl.GraphDSL.Implicits._
      val broadcast = builder.add(Broadcast[PartialFills](2))

      source ~> broadcast.in
      broadcast.out(0) ~> concatFlow ~> pDate // replace with fold of string
      broadcast.out(1) ~> lOrder
      ClosedShape
  }).run

  1 to 1000 foreach { _ => orderGateway ! generateRandomOrder }

  dates.foreach(r => println(s"Done >>>>> \n$r"))
}

object StockExchangeMat extends App with StrictLogging {
  val count = Flow[PartialFills].map(_.seq.length)
  val sumSink = Sink.fold[Int, Int](0)(_ + _)

  val sum = Source.fromPublisher(gatewayPublisher)
    //OrderSourceStub()
    .via(OrderIdGenerator())
    .via(OrderPersistence(orderDao))
    .via(OrderProcessor())
    .via(OrderExecutor())
    .via(count)
    .toMat(sumSink)(Keep.right)
    .run()

  sum.foreach(s => logger.info(s"sum is = $s"))
  sum.failed.foreach(s => logger.error(s"something went wrong = $s"))

  1 to 100 foreach { _ => orderGateway ! generateRandomOrder }
}

object StockExchangeGraph extends App {
  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
    import akka.stream.scaladsl.GraphDSL.Implicits._
    val bcast = b.add(Broadcast[PartialFills](2))

    Source.fromPublisher(gatewayPublisher)
      .via(OrderIdGenerator())
      .via(OrderPersistence(orderDao))
      .via(OrderProcessor())
      .via(OrderExecutor()) ~> bcast.in

    val concat = Flow[PartialFills].mapConcat(_.seq.toList)
    val printDate = Sink.foreach[ExecutedQuantity](eq => println(eq.executionDate))
    val logOrder = Sink.actorSubscriber(orderLogger)

    bcast.out(0) ~> concat ~> printDate
    bcast.out(1) ~> logOrder
    ClosedShape // will throw an exception if it is not really closed graph
  })
  g.run()

  1 to 1000 foreach { _ => orderGateway ! generateRandomOrder }
}

object StockExchange extends App {
  //OrderSourceStub()
  Source.fromPublisher(gatewayPublisher)
    .via(OrderIdGenerator())
    .via(OrderPersistence(orderDao))
    .via(OrderProcessor())
    .via(OrderExecutor())
    //.throttle(1, 1.second, 1, ThrottleMode.shaping)
    .runWith(Sink.actorSubscriber(orderLogger))

  // send orders to publisher actor
  1 to 1000 foreach { _ => orderGateway ! generateRandomOrder }
}

object OrderProcessor {
  def apply(): Flow[LoggedOrder, ExecuteOrder, NotUsed] =
    Flow.fromFunction(o => ExecuteOrder(o.orderId, o.order.quantity))
}

object OrderExecutor extends StrictLogging {
  val execQuantity = 3

  def apply(): Flow[ExecuteOrder, PartialFills, NotUsed] = Flow.fromFunction(o => execute(o))

  private def execute(eo: ExecuteOrder): PartialFills = {
    logger.info("Going to execute next order = {}", eo)

    val quantities = Seq.fill(execQuantity)(Random.nextInt(eo.quantity / execQuantity))
    PartialFills(quantities.map { q =>
      ExecutedQuantity(eo.orderId, q, LocalDateTime.now)
    })
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

object OrderSourceStub extends StrictLogging {
  val symbols = Array("APPL", "GOOG", "IBM", "YAH")

  def generateRandomOrder = new Order(
    OrderType(Random.nextInt(OrderType.values.size)),
    BigDecimal.valueOf(Random.nextDouble * 100),
    symbols(Random.nextInt(symbols.length)),
    Math.abs(Random.nextInt(Int.MaxValue)),
    Math.abs(100 + Random.nextInt(500)))


  def apply(): Source[Order, NotUsed] = {
    val orderGenerator: Iterable[Order] = new Iterable[Order] {
      var counter = 0

      override def iterator: Iterator[Order] = new Iterator[Order]() {
        override def hasNext: Boolean = {
          counter += 1
          logger.info("counter = " + counter)
          counter < 100
        }

        override def next(): Order = generateRandomOrder
      }
    }

    Source(orderGenerator)
  }
}