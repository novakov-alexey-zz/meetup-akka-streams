name := "meetup-akka-streams"

version := "1.0"

scalaVersion := "2.12.4"

val scalaLoggingVersion = "3.7.2"
val logbackVersion = "1.2.3"
val loggingScala = "com.typesafe.scala-logging" %% "scala-logging" % scalaLoggingVersion
val loggingLogback = "ch.qos.logback" % "logback-classic" % logbackVersion

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.5.6",
  "com.typesafe.akka" %% "akka-http" % "10.0.10",
  "org.json4s" %% "json4s-native" % "3.5.0",
  "mysql" % "mysql-connector-java" % "5.1.16",
  //  "org.mybatis.scala" % "mybatis-scala-core_2.11" % "1.0.3",
  "org.scalikejdbc" %% "scalikejdbc" % "3.1.0",
  "com.google.inject" % "guice" % "4.0",
  "cloud.drdrdr" %% "oauth-headers" % "0.3",
  loggingScala,
  loggingLogback
)