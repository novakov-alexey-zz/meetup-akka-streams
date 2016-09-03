package meetup.streams.ex2.exchange.dal

import java.sql.{CallableStatement, PreparedStatement, ResultSet, Timestamp}
import java.time.{LocalDateTime, ZoneId, ZonedDateTime}
import java.util.GregorianCalendar

import org.apache.ibatis.`type`.{BaseTypeHandler, JdbcType}


class LocalDateTimeTypeHandler extends BaseTypeHandler[LocalDateTime] {
  override def getNullableResult(rs: ResultSet, columnName: String): LocalDateTime =
    Option(rs.getTimestamp(columnName)).map(ts => LocalDateTime.ofInstant(ts.toInstant, ZoneId.systemDefault)).orNull

  override def getNullableResult(rs: ResultSet, columnIndex: Int): LocalDateTime =
    Option(rs.getTimestamp(columnIndex)).map(ts => LocalDateTime.ofInstant(ts.toInstant, ZoneId.systemDefault)).orNull

  override def getNullableResult(cs: CallableStatement, columnIndex: Int): LocalDateTime =
    Option(cs.getTimestamp(columnIndex)).map(ts => LocalDateTime.ofInstant(ts.toInstant, ZoneId.systemDefault)).orNull

  override def setNonNullParameter(ps: PreparedStatement, i: Int, parameter: LocalDateTime, jdbcType: JdbcType): Unit =
    ps.setTimestamp(i, Timestamp.valueOf(parameter), GregorianCalendar.from(ZonedDateTime.of(parameter, ZoneId.systemDefault)))
}
