package meetup.streams.ex2.exchange

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import meetup.streams.ex2.exchange.dal.OrderDao
import meetup.streams.ex2.exchange.om.{Execution, Order, PartialFills}
import org.scalatest.{FlatSpec, Matchers}

class StockExchangeTest extends FlatSpec with Matchers {
  implicit val system: ActorSystem = ActorSystem("StockExchange")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  it should "sink 3 ExecutedQuantity" in {
    val (pub, sub) = TestSource.probe[Order]
      .via(OrderIdGenerator())
      .via(OrderPersistence(orderDaoStub))
      .via(OrderProcessor())
      .via(OrderExecutor())
      .toMat(TestSink.probe[PartialFills])(Keep.both)
      .run

    sub.request(1)
    val order = OrderSourceStub.generateRandomOrder
    pub.sendNext(order)

    sub.expectNextPF {
      case PartialFills(l) =>
        l.length should be(3)
        l.head.orderId should ===(order.orderId)

      case m => fail(s"expected PartialFills element, but found $m")
    }
    pub.sendComplete()
    sub.expectComplete()
  }

  private def orderDaoStub = new OrderDao {
    override def saveOrder(order: Order): Unit = ()

    override def insertExecution(execution: Execution): Unit = ()
  }
}
