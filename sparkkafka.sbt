
name := "SparkKafka Project"

version := "1.0"
scalaVersion := "2.11.8"
val sparkVers = "2.3.1"

// Base Spark-provided dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVers % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVers % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1")

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("META-INF")  => MergeStrategy.discard
  case "reference.conf"                           => MergeStrategy.concat
  case m if m.endsWith("UnusedStubClass.class")   => MergeStrategy.discard
  case _ => MergeStrategy.first
}
