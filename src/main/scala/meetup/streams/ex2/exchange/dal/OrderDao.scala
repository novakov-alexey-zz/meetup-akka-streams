package meetup.streams.ex2.exchange.dal

import java.math.BigDecimal
import java.time.LocalDateTime

import meetup.streams.ex2.exchange.Config
import meetup.streams.ex2.exchange.om.{Execution, Order}
import meetup.streams.ex2.exchange.om.OrderType.OrderType
import org.mybatis.scala.mapping.{ResultMap, _}

object OrderDaoMapping {
  val completeBatch = new Update[(LocalDateTime, Long)] {
    override def xsql =
      """
        UPDATE OrderLog
        SET complete_batch_date = #{_1, typeHandler = meetup.streams.ex2.exchange.dal.LocalDateTimeTypeHandler}
        WHERE orderid <= #{_2}
      """
  }

  val selectOrders = new SelectList[OrderEntity] {
    resultMap = new ResultMap[OrderEntity] {
      id(property = "orderId", column = "orderId")
      result(property = "executionDate", column = "executionDate", typeHandler = T[LocalDateTimeTypeHandler])
      result(property = "orderType", column = "orderType", typeHandler = T[OrderTypeEnumTypeHandler])
      result(property = "executionPrice", column = "executionPrice")
      result(property = "symbol", column = "symbol")
      result(property = "userId", column = "userId")
      result(property = "quantity", column = "quantity")
    }

    override def xsql = "SELECT * FROM OrderLog"
  }

  val saveOrder = new Insert[OrderEntity] {
    override def xsql =
      <xsql>
        INSERT INTO OrderLog (orderId, executionDate, orderType, executionPrice, symbol, userId, quantity)
        VALUES (
        #{{orderId}},
        #{{executionDate, typeHandler = meetup.streams.ex2.exchange.dal.LocalDateTimeTypeHandler}},
        #{{orderType, typeHandler = meetup.streams.ex2.exchange.dal.OrderTypeEnumTypeHandler}},
        #{{executionPrice}}, #{{symbol}}, #{{userId}}, #{{quantity}}
        )
      </xsql>
  }

  val insertExecution = new Insert[Execution] {
    override def xsql =
      <xsql>
        INSERT INTO OrderExecution (orderId, executionDate, quantity)
        VALUES (#{{orderId}}, #{{executionDate, typeHandler = meetup.streams.ex2.exchange.dal.LocalDateTimeTypeHandler}}, #{{quantity}})
      </xsql>
  }

  def bind: Seq[Statement] = Seq(selectOrders, saveOrder, completeBatch, insertExecution)
}

class OrderEntity {
  var orderId: Long = _
  var executionDate: LocalDateTime = _
  var orderType: OrderType = _
  var executionPrice: BigDecimal = _
  var symbol: String = _
  var userId: Int = _
  var quantity: Int = _

  def toOrder = Order(orderId, executionDate, orderType, executionPrice, symbol, userId, quantity)
}

object OrderEntity {
  def toOrderEntity(order: Order): OrderEntity = new OrderEntity {
    orderId = order.orderId
    executionDate = order.executionDate
    orderType = order.orderType
    executionPrice = order.executionPrice
    symbol = order.symbol
    userId = order.userId
    quantity = order.quantity
  }
}

trait IOrderDao {
  def completeBatch(upToId: Long, withDate: LocalDateTime)

  def saveOrder(order: Order)

  def getOrders: Seq[Order]

  def insertExecution(execution: Execution)
}

class OrderDaoImpl extends IOrderDao {
  val db = Config.persistenceContext

  override def getOrders: Seq[Order] = db.readOnly { implicit session => OrderDaoMapping.selectOrders().map(_.toOrder) }

  override def completeBatch(upToId: Long, withDate: LocalDateTime): Unit =
    db.transaction { implicit session => OrderDaoMapping.completeBatch((withDate, upToId)) }

  override def saveOrder(order: Order): Unit =
    db.transaction { implicit session => OrderDaoMapping.saveOrder(OrderEntity.toOrderEntity(order)) }

  override def insertExecution(execution: Execution): Unit =
    db.transaction { implicit session => OrderDaoMapping.insertExecution(execution) }
}
