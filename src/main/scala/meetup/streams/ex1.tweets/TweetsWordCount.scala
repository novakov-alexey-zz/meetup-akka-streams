package meetup.streams.ex1.tweets

import java.util.concurrent.TimeoutException

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model._
import akka.stream.scaladsl.{RestartSource, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import cats.implicits._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.StrictLogging
import meetup.streams.ex1.tweets.Params._
import meetup.streams.ex1.tweets.om.Tweet
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.immutable._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.control.NonFatal

object Params {
  val stopWords = Set("rt", "the", "of", "with", "for", "from", "on", "at", "to", "in", "been", "is", "be", "was", "la",
    "were", "will", "a", "el", "that", "then", "than", "you", "e", "and", "or", "within", "without", "an", "que", "we",
    "se", "are", "must", "should", "have", "has", "had", "this", "all", "our", "y", "o", "i", "una", "by", "com", "las",
    "no", "who", "what", "los", "su", "cu", "de", "pra", "en", "usa", "es", "as", "my", "me", "eu", "out", "us", "your",
    "mine", "just", "da", "not", "more", "he", "para", "less", "he", "she", "do", "did", "germany", "al", "faz", "new",
    "muito", "cmg", "mulher", "con", "there", "so", "por", "now", "un", "having", "it", "when", "what", "where", "does",
    "they", "about", "del", "but", "loco", "sobre", "their", "over", "some", "get", "only", "tu", "essa", "if", "would",
    "q", "lo", "te", "can", "te", "up", "vale", "same", "last", "u", "r", "x")

  val requestParams = Map("track" -> "Germany,Spain,USA,Ukraine")

  val docDelimiter = "\r\n"

  val uniqueBuckets = 500
  val topCount = 15
  val idleDuration: FiniteDuration = 90 seconds
}

object TweetsWordCount extends App with StrictLogging {
  // json
  implicit val formats: DefaultFormats.type = DefaultFormats

  // akka
  implicit val system: ActorSystem = ActorSystem("TweetsWordCount")

  val decider: Supervision.Decider = {
    case _: TimeoutException => Supervision.Restart
    case NonFatal(e) =>
      logger.error(s"Stream failed with ${e.getMessage}, going to resume")
      Supervision.Resume
  }

  implicit val materializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(decider))

  val conf = ConfigFactory.load()
  val oAuthHeader = OAuthHeader(conf, requestParams)
  val httpRequest = createHttpRequest(oAuthHeader, Uri(conf.getString("twitter.url")))

  val restartSource = RestartSource.withBackoff(
    minBackoff = 3.seconds,
    maxBackoff = 30.seconds,
    randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
  ) { () =>
    Source.fromFutureSource {
      val response = Http().singleRequest(httpRequest)

      response.failed.foreach(t => System.err.println(s"Request has been failed with $t"))

      response.map(resp => {
        resp.status match {
          case OK => resp.entity.withoutSizeLimit().dataBytes
          case code =>
            val text = resp.entity.dataBytes.map(_.utf8String)
            val error = s"Unexpected status code: $code, $text"
            System.err.println(error)
            Source.failed(new RuntimeException(error))
        }
      })
    }
  }

  restartSource
    .idleTimeout(idleDuration)
    .scan("")((acc, curr) =>
      if (acc.contains(docDelimiter)) curr.utf8String
      else acc + curr.utf8String
    )
    .filter(_.contains(docDelimiter)).async
    .log("json", s => s)
    .map(json => parse(json).extract[Tweet])
    .log("created at", _.created_at)
    //.mapConcat[String](_.entities.hashtags.map(_.text).to[Iterable]) // hashtags
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

  private def tweetWordCount(text: String): Map[String, Int] = {
    text.split(" ")
      .filter(s => s.trim.nonEmpty && s.matches("\\w+"))
      .map(_.toLowerCase.trim)
      .filterNot(stopWords.contains)
      .foldLeft(Map.empty[String, Int]) {
        (count, word) => count |+| Map(word -> 1)
      }
  }

  /*
    See more details at twitter Streaming API
   */
  private def createHttpRequest(header: String, source: Uri): HttpRequest = {
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
        string = requestParams.map { case (k, v) => s"$k=$v" }.mkString(",")
      ).withoutSizeLimit()
    )
  }
}