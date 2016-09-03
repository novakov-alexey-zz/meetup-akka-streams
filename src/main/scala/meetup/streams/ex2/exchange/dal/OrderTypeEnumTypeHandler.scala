package meetup.streams.ex2.exchange.dal

import java.sql.{CallableStatement, PreparedStatement, ResultSet}

import meetup.streams.ex2.exchange.om.OrderType
import org.apache.ibatis.`type`.{BaseTypeHandler, JdbcType}

class OrderTypeEnumTypeHandler extends BaseTypeHandler[OrderType.OrderType] {
  override def getNullableResult(rs: ResultSet, columnName: String) = OrderType.withName(rs.getString(columnName))

  override def getNullableResult(rs: ResultSet, columnIndex: Int) = OrderType.withName(rs.getString(columnIndex))

  override def getNullableResult(cs: CallableStatement, columnIndex: Int) = OrderType.withName(cs.getString(columnIndex))

  override def setNonNullParameter(ps: PreparedStatement, i: Int, parameter: OrderType.OrderType, jdbcType: JdbcType): Unit =
    ps.setString(i, parameter.toString)
}
