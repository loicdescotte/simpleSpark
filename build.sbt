name := "Simple Project"

version := "1.0"

scalaVersion := "2.11.7"

resolvers += Resolver.sonatypeRepo("releases")

val framelessVersion = "0.2.0"

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-core" % "2.0.2",
  "org.apache.spark" %% "spark-sql" % "2.0.2",
  "io.github.adelbertc" %% "frameless-cats"      % framelessVersion,
  "io.github.adelbertc" %% "frameless-dataset"   % framelessVersion,
  //"org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
