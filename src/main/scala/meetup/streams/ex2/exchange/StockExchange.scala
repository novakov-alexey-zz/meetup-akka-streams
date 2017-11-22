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
import meetup.streams.ex2.exchange.dal.OrderDao
import meetup.streams.ex2.exchange.om.{ExecutedQuantity, _}
import meetup.streams.ex2.exchange.stages.{OrderGateway, OrderLoggerStage, RandomOrderSource}
import org.reactivestreams.Publisher

import scala.collection.immutable.Iterable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Random

object Common {
  implicit val system: ActorSystem = ActorSystem("StockExchange")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val orderDao: OrderDao = Config.injector.getInstance(classOf[OrderDao])
  val orderLogger = new OrderLoggerStage(orderDao)

  val orderGateway: ActorRef = system.actorOf(Props[OrderGateway])
  val orderPublisher: Publisher[Order] = ActorPublisher[Order](orderGateway)
}

object StockExchangeGraphPool extends App {
  val workers = 5
  val orderSource = new RandomOrderSource(100)

  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
    import akka.stream.scaladsl.GraphDSL.Implicits._
    val bcast = b.add(Broadcast[PreparedOrder](2))
    val balancer = b.add(Balance[PartialFills](workers))

    val S = b.add(Source.fromGraph(orderSource))
    val IdGen = b.add(OrderIdGenerator())
    val A = b.add(OrderPersistence(orderDao).to(Sink.ignore))
    val B = b.add(OrderProcessor2())
    val C = b.add(OrderExecutor())

    S ~> IdGen ~> bcast
                  bcast ~> A
                  bcast ~> B ~> C ~> balancer

    for (i <- 0 until workers)
      balancer ~> b.add(Sink.fromGraph(orderLogger).named(s"logger-$i"))

    ClosedShape
  })

  g.run()
}

object StockExchangeGraphMat extends App {
  // Source
  val source = Source.fromPublisher(orderPublisher)
    .via(OrderIdGenerator())
    .via(OrderPersistence(orderDao))
    .via(OrderProcessor())
    .via(OrderExecutor())

  // Flow
  val concatFlow = Flow[PartialFills].mapConcat(_.seq.toList)

  // Sinks
  val foldQuantity = Sink.fold[Long, ExecutedQuantity](0)((s, eq) => s + eq.quantity)
  val logOrder = Sink.fromGraph(orderLogger)

  val (quantity, _) = RunnableGraph.fromGraph(GraphDSL.create(foldQuantity, logOrder)((_, _)) { implicit builder =>
    (console, persistence) =>
      import akka.stream.scaladsl.GraphDSL.Implicits._

      val bcast = builder.add(Broadcast[PartialFills](2))

      // Graph
      source ~> bcast.in
      bcast.out(0) ~> concatFlow ~> console // replace with fold of string
      bcast.out(1) ~> persistence

      ClosedShape
  }).run

  1 to 1000 foreach { _ => orderGateway ! generateRandomOrder }

  quantity.foreach(r => println(s"Done >>>>> \n traded quantity: $r"))
}

object StockExchangeMat extends App with StrictLogging {
  val count = Flow[PartialFills].map(_.seq.length)
  val sumSink = Sink.fold[Int, Int](0)(_ + _)

  val sum = Source.fromPublisher(orderPublisher)
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
  sum.onComplete(_ => {
    logger.info("Shutdown ActorSystem")
    system.terminate()
  })

  1 to 100 foreach { _ => orderGateway ! generateRandomOrder }
}

object StockExchangeGraph extends App {
  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit b =>
    import akka.stream.scaladsl.GraphDSL.Implicits._
    val bcast = b.add(Broadcast[PartialFills](2))

    Source.fromPublisher(orderPublisher)
      .via(OrderIdGenerator())
      .via(OrderPersistence(orderDao))
      .via(OrderProcessor())
      .via(OrderExecutor()) ~> bcast.in

    val concat = Flow[PartialFills].mapConcat(_.seq.toList)
    val console = Sink.foreach[ExecutedQuantity](eq => println(eq.executionDate))
    val persistence = Sink.fromGraph(orderLogger)

    bcast.out(0) ~> concat ~> console
    bcast.out(1) ~> persistence

    ClosedShape // will throw an exception if it is not really closed graph
  })
  g.run()

  1 to 1000 foreach { _ => orderGateway ! generateRandomOrder }
}

object StockExchange extends App {
  //OrderSourceStub()
  Source.fromPublisher(orderPublisher)
    .via(OrderIdGenerator())
    .via(OrderPersistence(orderDao))
    .via(OrderProcessor())
    .via(OrderExecutor())
    //.throttle(1, 1.second, 1, ThrottleMode.shaping)
    .runWith(Sink.fromGraph(orderLogger))

  // send orders to publisher stage
  1 to 1000 foreach { _ => orderGateway ! generateRandomOrder }
}

object OrderProcessor {
  def apply(): Flow[LoggedOrder, ExecuteOrder, NotUsed] =
    Flow.fromFunction(o => ExecuteOrder(o.orderId, o.order.quantity))
}

object OrderProcessor2 {
  def apply(): Flow[PreparedOrder, ExecuteOrder, NotUsed] =
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
  def apply(orderDao: OrderDao): Flow[PreparedOrder, LoggedOrder, NotUsed] =
    Flow.fromFunction(p => {
      orderDao.saveOrder(new Order(p.orderId, p.order))
      LoggedOrder(p.orderId, p.order)
    })
}

object OrderIdGenerator {
  def apply(): Flow[Order, PreparedOrder, NotUsed] = {
    var seqNo = 0L

    def nextSeqNo(): Long = {
      seqNo += 1
      seqNo
    }

    Flow.fromFunction(o => PreparedOrder(o, nextSeqNo()))
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