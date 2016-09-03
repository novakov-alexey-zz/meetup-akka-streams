package meetup.streams.ex1.tweets

import akka.actor.ActorSystem
import com.hunorkovacs.koauth.domain.KoauthRequest
import com.hunorkovacs.koauth.service.consumer.DefaultConsumerService
import com.typesafe.config.Config

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object OAuthHeader {
  def apply(conf: Config, reqBody: String)(implicit system: ActorSystem): Future[String] = {
    val consumer = new DefaultConsumerService(system.dispatcher)

    consumer.createOauthenticatedRequest(
      KoauthRequest(
        method = "POST",
        url = conf.getString("twitter.url"),
        authorizationHeader = None,
        body = Some(reqBody)
      ),
      conf.getString("twitter.consumerKey"),
      conf.getString("twitter.consumerSecret"),
      conf.getString("twitter.accessToken"),
      conf.getString("twitter.accessTokenSecret")
    ) map (_.header)
  }
}
