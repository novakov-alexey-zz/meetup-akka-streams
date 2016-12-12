name := "meetup-akka-streams"

version := "1.0"

scalaVersion := "2.11.8"

val scalaLoggingVersion = "3.1.0"
val logbackVersion = "1.1.2"
val loggingScala    = "com.typesafe.scala-logging"  %% "scala-logging"                  % scalaLoggingVersion
val loggingLogback  = "ch.qos.logback"              %  "logback-classic"                % logbackVersion

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % "2.4.14",
  "com.typesafe.akka" %% "akka-http" % "10.0.0",
  "org.json4s" %% "json4s-native" % "3.5.0",
  "com.hunorkovacs" %% "koauth" % "1.1.0",
  "mysql" % "mysql-connector-java" % "5.1.16",
  "org.mybatis.scala" % "mybatis-scala-core_2.11" % "1.0.3",
  "com.google.inject" % "guice" % "4.0",
  loggingScala,
  loggingLogback
)