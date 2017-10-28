package meetup.streams.ex1.tweets

import akka.actor.ActorSystem
import cloud.drdrdr.oauth._
import com.typesafe.config.Config

object OAuthHeader {
  def apply(conf: Config, params: Map[String, String])(implicit system: ActorSystem): String = {
    val oauth = new Oauth(key = conf.getString("twitter.consumerKey"), secret = conf.getString("twitter.consumerSecret"))
    oauth.setAccessTokens(conf.getString("twitter.accessToken"), conf.getString("twitter.accessTokenSecret"))
    oauth.getSignedHeader(conf.getString("twitter.url"), "POST", params)
  }
}
