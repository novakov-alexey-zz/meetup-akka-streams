package meetup.streams.ex2.exchange

import com.google.inject.{AbstractModule, Guice}
import meetup.streams.ex2.exchange.dal.{IOrderDao, OrderDaoImpl, OrderDaoMapping}
import org.mybatis.scala.config.Configuration

object Config {
  def createMybatisConfig(): Configuration = Configuration("mybatis.xml").
    addSpace("meetup.akka.dal.OrderDao") { space ⇒
      space ++= OrderDaoMapping
    }

  val persistenceContext = createMybatisConfig().createPersistenceContext
  val injector = Guice.createInjector(new OrderProcessorModule)
}

class OrderProcessorModule extends AbstractModule {
  override def configure(): Unit = bind(classOf[IOrderDao]) to classOf[OrderDaoImpl]
}