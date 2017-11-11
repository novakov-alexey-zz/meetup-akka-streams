package meetup.streams.ex1.tweets

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpHeader.ParsingResult
import akka.http.scaladsl.model.StatusCodes.OK
import akka.http.scaladsl.model._
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import cats.implicits._
import com.typesafe.config.ConfigFactory
import meetup.streams.ex1.tweets.om.Tweet
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.immutable.ListMap
import scala.concurrent.ExecutionContext.Implicits.global

object TweetsFilter extends App {
  implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val system: ActorSystem = ActorSystem()

  val decider: Supervision.Decider = _ => Supervision.Resume
  implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system)
    .withSupervisionStrategy(decider))

  val conf = ConfigFactory.load()
  val params = Map("track" -> "USA,Germany,Ukraine")

  val oAuthHeader = OAuthHeader(conf, params)
  val source = Uri(conf.getString("twitter.url"))
  val httpRequest = createHttpRequest(oAuthHeader, source)
  val stopWords = Set("rt", "the", "of", "with", "for", "from", "on", "at", "to", "in", "been", "is", "be", "was", "la",
    "were", "will", "a", "el", "that", "then", "than", "you", "e", "and", "or", "within", "without", "an", "que", "we",
    "se", "are", "must", "should", "have", "has", "had", "this", "all", "our", "y", "o", "i", "una", "by", "com", "las",
    "no", "who", "what", "los", "su", "cu", "de", "pra", "en", "usa", "es", "as", "my", "me", "eu", "out", "us", "your",
    "mine", "just", "da", "not", "more", "he", "para", "less", "he", "she", "do", "did", "germany", "al", "faz", "new",
    "muito", "cmg", "mulher", "con", "there", "so", "por", "now", "un", "having", "it", "when", "what", "where", "does",
    "they", "about", "del", "but", "loco", "sobre", "their", "over", "some", "get", "only", "tu", "essa")

  val response = Http().singleRequest(httpRequest)

  response.foreach { resp =>
    resp.status match {
      case OK =>
        // Source
        resp.entity.dataBytes
          .scan("")((acc, curr) =>
            if (acc.contains("\r\n")) curr.utf8String
            else acc + curr.utf8String
          )
          .filter(_.contains("\r\n"))
          .map(json => parse(json).extract[Tweet].text)
          .scan(Map.empty[String, Int]) {
            (acc, text) => {
              val wc = text.split(" ")
                .filter(s => s.nonEmpty && s.matches("\\w+"))
                .map(_.toLowerCase)
                .filterNot(stopWords.contains)
                .foldLeft(Map.empty[String, Int]) {
                  (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
                }
              ListMap((acc combine wc).toSeq.sortBy(_._2)(Ordering[Int].reverse).take(500): _*)
            }
          }
          // Sink
          .runForeach { wc =>
          val stats = wc.take(15).map(a => a._1 + ":" + a._2).mkString(" ")
          print("\r" + stats)
        }

      case _ => resp.entity.dataBytes.runForeach(bs => println(bs.utf8String))
    }
  }

  response.failed.foreach(t => System.err.println(t))

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
        string = params.map { case (k, v) => k + "=" + v }.mkString(",")
      )
    )
  }
}