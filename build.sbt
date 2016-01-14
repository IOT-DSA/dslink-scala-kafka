// properties

val APP_VERSION = "0.1.0-SNAPSHOT"

val SCALA_VERSION = "2.10.5"

val DSA_VERSION = "0.13.0"

val KAFKA_VERSION = "0.8.2.2"

// settings

name := "dslink-scala-kafka"

organization := "org.iot-dsa"

version := APP_VERSION

enablePlugins(JavaAppPackaging)

scalaVersion := SCALA_VERSION

scalacOptions ++= Seq("-feature", "-unchecked", "-deprecation", "-Xlint", 
	"-Ywarn-dead-code", "-language:_", "-target:jvm-1.7", "-encoding", "UTF-8")

run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

// publishing options

publishMavenStyle := true
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
pomIncludeRepository := { _ => false }
pomExtra := (
  <url>https://github.com/IOT-DSA/dslink-scala-kafka</url>
  <licenses>
    <license>
      <name>The Apache License, Version 2.0</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>scm:git:https://github.com/IOT-DSA/dslink-scala-kafka.git</url>
    <connection>scm:git:git@github.com:IOT-DSA/dslink-scala-kafka.git</connection>
  </scm>
  <developers>
    <developer>
      <id>snark</id>
      <name>Vlad Orzhekhovskiy</name>
      <email>vlad@uralian.com</email>
      <url>http://uralian.com</url>
    </developer>
  </developers>)
	
libraryDependencies ++= Seq(
  "org.iot-dsa"         % "dslink"                  % DSA_VERSION,
  "com.typesafe"        % "config"                  % "1.2.1",
  "org.apache.kafka"   %% "kafka"                   % KAFKA_VERSION
      exclude("javax.jms", "jms")
      exclude("com.sun.jdmk", "jmxtools")
      exclude("com.sun.jmx", "jmxri"),
  "org.scalatest"      %% "scalatest"               % "2.2.1"         % "test",
  "org.scalacheck"     %% "scalacheck"              % "1.12.1"        % "test"  
)