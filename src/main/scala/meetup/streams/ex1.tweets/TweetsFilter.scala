package meetup.streams.ex1.tweets

import java.util.concurrent.TimeoutException

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Attributes, Supervision}
import cats.implicits._
import com.typesafe.config.ConfigFactory
import meetup.streams.ex1.tweets.om.Tweet
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.immutable.ListMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object TweetsFilter extends App {
  // json
  implicit val formats: DefaultFormats.type = DefaultFormats

  // akka
  implicit val system: ActorSystem = ActorSystem()
  val decider: Supervision.Decider = {
    case _: TimeoutException => Supervision.Restart // also, need to re-connect as per Twitter API spec
    case _ => Supervision.Resume
  }
  implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system)
    .withSupervisionStrategy(decider))

  val conf = ConfigFactory.load()
  val params = Map("track" -> "Twitter")

  val stopWords = Set("rt", "the", "of", "with", "for", "from", "on", "at", "to", "in", "been", "is", "be", "was", "la",
    "were", "will", "a", "el", "that", "then", "than", "you", "e", "and", "or", "within", "without", "an", "que", "we",
    "se", "are", "must", "should", "have", "has", "had", "this", "all", "our", "y", "o", "i", "una", "by", "com", "las",
    "no", "who", "what", "los", "su", "cu", "de", "pra", "en", "usa", "es", "as", "my", "me", "eu", "out", "us", "your",
    "mine", "just", "da", "not", "more", "he", "para", "less", "he", "she", "do", "did", "germany", "al", "faz", "new",
    "muito", "cmg", "mulher", "con", "there", "so", "por", "now", "un", "having", "it", "when", "what", "where", "does",
    "they", "about", "del", "but", "loco", "sobre", "their", "over", "some", "get", "only", "tu", "essa", "if", "would",
    "q", "lo", "te", "can", "te", "up", "vale", "same", "last", "u", "r", "x")

  val oAuthHeader = OAuthHeader(conf, params)
  val httpRequest = createHttpRequest(oAuthHeader, Uri(conf.getString("twitter.url")))
  val uniqueBuckets = 500
  val topCount = 15

  val response = Http().singleRequest(httpRequest)

  response.foreach { resp =>
    resp.status match {
      case OK =>
        val source = resp.entity.withoutSizeLimit().dataBytes

        source
          .idleTimeout(90 seconds)
          .scan("")((acc, curr) =>
            if (acc.contains("\r\n")) curr.utf8String
            else acc + curr.utf8String
          )
          .filter(_.contains("\r\n")).async
          .map(json => parse(json).extract[Tweet])
          .log("created at", _.created_at)
          .withAttributes(Attributes.logLevels(Logging.DebugLevel))
          .map(_.text)
          .log("tweet", t => t.take(20) + "...")
          .scan(Map.empty[String, Int]) {
            (acc, text) => {
              val wc = tweetWordCount(text)
              ListMap((acc |+| wc).toSeq.sortBy(-_._2).take(uniqueBuckets): _*)
            }
          }
          .runForeach { wc =>
            val stats = wc.take(topCount).map { case (k, v) => s"$k:$v" }.mkString("  ")
            print("\r" + stats)
          }

      case _ => resp.entity.dataBytes.runForeach(bs => println(bs.utf8String))
    }
  }

  private def tweetWordCount(text: String): Map[String, Int] = {
    text.split(" ")
      .filter(s => s.trim.nonEmpty && s.matches("\\w+"))
      .map(_.toLowerCase.trim)
      .filterNot(stopWords.contains)
      .foldLeft(Map.empty[String, Int]) {
        (count, word) => count |+| Map(word -> 1)
      }
  }

  response.failed.foreach(System.err.println(_))

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
        string = params.map { case (k, v) => s"$k=$v" }.mkString(",")
      ).withoutSizeLimit()
    )
  }
}