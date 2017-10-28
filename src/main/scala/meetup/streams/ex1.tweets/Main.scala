package meetup.streams.ex1.tweets

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import meetup.streams.ex1.tweets.om.Tweet
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

object Main extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val formats: DefaultFormats.type = DefaultFormats

  val conf = ConfigFactory.load()
  val body = Map("track" -> "Ukraine")

  val oAuthHeader = OAuthHeader(conf, body)
  val source = Uri(conf.getString("twitter.url"))
  val httpRequest = createHttpRequest(oAuthHeader, source)

  Http().singleRequest(httpRequest).map { response =>
    response.status match {
      case OK =>
        // Stream begins
        response.entity.dataBytes // Source
          .scan("")((acc, curr) => if (acc.contains("\r\n")) curr.utf8String else acc + curr.utf8String)
          .filter(s =>
            s.trim.nonEmpty && s.contains("\r\n")
          )
          .map(json => Try(parse(json).extract[Tweet]))
          .runForeach {
            case Success(tweet) => println("----\n" + tweet.text)
            case Failure(e) => println("-----\n" + e.getMessage)
          }
      // Stream ends
      case _ => println(response.entity.dataBytes.runForeach(_.utf8String))
    }
  }

  /*
    See more details at twitter Streaming API
   */
  def createHttpRequest(header: String, source: Uri): HttpRequest = {
    val httpHeaders = List(
      HttpHeader.parse("Authorization", header) match {
        case ParsingResult.Ok(h, _) => Some(h)
        case _ => None
      },
      HttpHeader.parse("Accept", "*/*") match {
        case ParsingResult.Ok(h, _) => Some(h)
        case _ => None
      }
    ).flatten

    HttpRequest(
      method = HttpMethods.POST,
      uri = source,
      headers = httpHeaders,
      entity = HttpEntity(
        contentType = ContentType(MediaTypes.`application/x-www-form-urlencoded`, HttpCharsets.`UTF-8`),
        string = body.map { case (k, v) => k + "=" + v }.mkString(",")
      )
    )
  }
}