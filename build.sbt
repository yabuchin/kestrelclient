
name := "kestrelclient"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "twitter.com" at "http://maven.twttr.com/"

libraryDependencies ++= Seq(
  "com.twitter" % "finagle-core_2.9.1" % "3.0.0",
  "com.twitter" % "finagle-kestrel_2.9.1" % "3.0.0",
  "com.twitter" % "finagle-memcached_2.9.1" % "3.0.0",
  "org.scalatest" %% "scalatest" % "1.7.2" % "test"
)
