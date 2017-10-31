package meetup.streams.ex2.exchange

import com.google.inject.{AbstractModule, Guice, Injector}
import meetup.streams.ex2.exchange.dal.{OrderDao, OrderDaoImpl}
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

object Config {
  val injector: Injector = Guice.createInjector(new OrderProcessorModule)
}

class OrderProcessorModule extends AbstractModule {

  override def configure(): Unit =
    bind(classOf[OrderDao]) toInstance {
      val settings = ConnectionPoolSettings(
        initialSize = 5,
        maxSize = 20,
        connectionTimeoutMillis = 3000L,
        validationQuery = "select 1 from dual")

      val connectionPoolName = "orders"

      ConnectionPool.add(connectionPoolName,
        "jdbc:mysql://localhost:3306/somedb?autoReconnect=true&amp;useSSL=false", "root", "root", settings)

      new OrderDaoImpl(connectionPoolName)
    }
}