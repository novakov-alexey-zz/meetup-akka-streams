package meetup.streams.ex2.exchange.dal

import java.math.BigDecimal
import java.time.LocalDateTime

import meetup.streams.ex2.exchange.om.OrderType.OrderType
import meetup.streams.ex2.exchange.om.{Execution, Order}
import scalikejdbc._

case class OrderEntity(
                        orderid: Long,
                        executiondate: LocalDateTime,
                        ordertype: OrderType,
                        executionprice: BigDecimal,
                        symbol: String,
                        userid: Int,
                        quantity: Int)

class OrderEntitySql(override val connectionPoolName: String) extends SQLSyntaxSupport[OrderEntity] {
  override def tableName = "OrderLog"
}

case class ExecutionEntity(orderid: Long, quantity: Int, executiondate: LocalDateTime)

class ExecutionEntitySql(override val connectionPoolName: String) extends SQLSyntaxSupport[ExecutionEntity] {
  override def tableName = "OrderExecution"
}

trait OrderDao {
  def saveOrder(order: Order)

  def insertExecution(execution: Execution)
}

class OrderDaoImpl(poolName: String) extends OrderDao {
  implicit val orderTypeParameterBinderFactory: ParameterBinderFactory[OrderType] = ParameterBinderFactory {
    orderType => (stmt, idx) => stmt.setString(idx, orderType.toString)
  }

  private val executionEntitySql = new ExecutionEntitySql(poolName)
  private val eeCol = executionEntitySql.column
  private val orderEntitySql = new OrderEntitySql(poolName)
  private val oeCol = orderEntitySql.column

  def db: NamedDB = NamedDB(poolName)

  override def saveOrder(order: Order): Unit = {
    db.localTx { session =>
      withSQL {
        insert.into(orderEntitySql).namedValues(
          oeCol.orderid -> order.orderId,
          oeCol.executiondate -> order.executionDate,
          oeCol.ordertype -> order.orderType,
          oeCol.quantity -> order.quantity,
          oeCol.executionprice -> order.executionPrice,
          oeCol.symbol -> order.symbol,
          oeCol.userid -> order.userId
        )
      }.update.apply()(session)
    }
  }

  override def insertExecution(execution: Execution): Unit = {
    db.localTx { session =>
      withSQL {
        insert.into(executionEntitySql).namedValues(
          eeCol.orderid -> execution.orderId,
          eeCol.executiondate -> execution.executionDate,
          eeCol.quantity -> execution.quantity
        )
      }.update().apply()(session)
    }
  }
}
