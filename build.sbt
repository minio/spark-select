name := "spark-select"

organization := "io.minio"

scalaVersion := "2.11.12"

crossScalaVersions := Seq("2.11.12")

version := "0.0.1"

spName := "minio/spark-select"

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

sparkVersion := "2.3.1"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion := sys.props.get("spark.testVersion").getOrElse(sparkVersion.value)

// used spark components
sparkComponents := Seq("sql")

// Dependent libraries
libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk" % "1.11.434" exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.apache.commons" % "commons-csv" % "1.1",
  "org.mockito" % "mockito-core" % "2.0.31-beta"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "test" force(),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % "test"  force(),
  "org.scala-lang" % "scala-library" % scalaVersion.value % "compile"
)

/**
 * release settings
 */
publishMavenStyle := true

releaseCrossBuild := true

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

releasePublishArtifactsAction := PgpKeys.publishSigned.value

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

pomExtra := (
  <url>https://github.com/minio/spark-select</url>
    <licenses>
      <license>
        <name>Apache License, Verision 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:github.com/minio/spark-select</connection>
      <developerConnection>scm:git:git@github.com:minio/spark-select</developerConnection>
      <url>github.com/minio/spark-select</url>
    </scm>
    <developers>
      <developer>
        <id>minio</id>
        <name>Minio</name>
        <url>http://www.minio.io</url>
      </developer>
    </developers>)

// Skip tests during assembly
test in assembly := {}

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}

import ReleaseTransformations._

// Add publishing to spark packages as another step.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges,
  releaseStepTask(spPublish)
)
