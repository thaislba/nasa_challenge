/**
 * @author: Thais Larissa Batista de Andrade
 * */
lazy val commonSettings = Seq(
  name := "nasa_challenge",
  version := "1.0",
  organization := "br.com.smtx",
  scalaVersion := "2.11.8",
  test in assembly := {},
)
val sparkVersion = "2.3.4"
lazy val app = (project in file("."))
  .settings(commonSettings: _*)
  .settings(

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion
      , "org.apache.spark" %% "spark-sql" % sparkVersion
      , "org.apache.spark" %% "spark-hive" % sparkVersion
      , "org.apache.spark" %% "spark-mllib" % sparkVersion
      , "org.scalatest" %% "scalatest" % "3.0.1" % "test"
    ),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
      case x => MergeStrategy.first
    }
    , parallelExecution in Test := false
    , fork in Test := false
    , javaOptions ++= Seq("-Xms4G", "-Xss8m", "-Xss2048K", "-Xmx2048M", "-Xlog-implicits","-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
  )
