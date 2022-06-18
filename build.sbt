name := "MB"
version := "1.0"
//course := "reactive"
//assignment := "actorbintree"

Test / parallelExecution := false

val AkkaVersion = "2.6.18"

scalaVersion := "3.1.0"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked"
)

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test,
  "org.scalameta" %% "munit" % "0.7.26" % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.11",
)
libraryDependencies += "org.reactivemongo" % "play2-reactivemongo_3" % "1.1.0-play28-RC4"

Compile / run / mainClass := Some("Producer.Producer")