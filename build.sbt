
name := "kestrelclient"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "twitter.com" at "http://maven.twttr.com/"

libraryDependencies ++= Seq(
  "com.twitter" % "finagle-core_2.9.1" % "3.0.0" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  ),
  "com.twitter" % "finagle-kestrel_2.9.1" % "3.0.0" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  ),
  "com.twitter" % "util_2.9.1" % "3.0.0" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  )
)
