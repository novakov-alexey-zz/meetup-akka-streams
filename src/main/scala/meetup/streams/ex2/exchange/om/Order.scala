package meetup.streams.ex2.exchange.om

import java.math.BigDecimal
import java.time.LocalDateTime

import meetup.streams.ex2.exchange.om.OrderType.OrderType

case class Order(orderId: Long = -1, executionDate: LocalDateTime, orderType: OrderType, executionPrice: BigDecimal,
                 symbol: String, userId: Int, quantity: Int) {
  def this(orderType: OrderType, executionPrice: BigDecimal, symbol: String, userId: Int, quantity: Int) =
    this(-1, LocalDateTime.now, orderType, executionPrice, symbol, userId, quantity)

  def this(orderId: Long, order: Order) =
    this(orderId, order.executionDate, order.orderType, order.executionPrice, order.symbol, order.userId, order.quantity)
}

case class PreparedOrder(order: Order, orderId: Long)

case class LoggedOrder(orderId: Long, order: Order)

case class Execution(orderId: Long, quantity: Int, executionDate: LocalDateTime)

case class ExecuteOrder(orderId: Long, quantity: Int)

case class ExecutedQuantity(orderId: Long, quantity: Int, executionDate: LocalDateTime)

case class ExecutionAck(orderId: Long, quantity: Int, executionId: Long)

case class AllAcksReceived(replies: Seq[ExecutionAck])