package meetup.streams.ex2.exchange

import com.google.inject.{AbstractModule, Guice, Injector}
import meetup.streams.ex2.exchange.dal.{IOrderDao, OrderDaoImpl, OrderDaoMapping}
import org.mybatis.scala.config.Configuration
import org.mybatis.scala.session.SessionManager

object Config {
  def createMybatisConfig(): Configuration = Configuration("mybatis.xml").
    addSpace("meetup.akka.dal.OrderDao") { space ⇒
      space ++= OrderDaoMapping
    }

  val persistenceContext: SessionManager = createMybatisConfig().createPersistenceContext
  val injector: Injector = Guice.createInjector(new OrderProcessorModule)
}

class OrderProcessorModule extends AbstractModule {
  override def configure(): Unit = bind(classOf[IOrderDao]) to classOf[OrderDaoImpl]
}