/*
 * Copyright 2015-2019 Snowflake Computing
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.util.Properties

val sparkVersion = "3-3-2-aiq75"
val testSparkVersion = sys.props.get("spark.testVersion").getOrElse(sparkVersion)
val defaultScalaVersion = "2.12.15"

// increment this version when making a new release
val sparkConnectorVersion = "2.11.3-aiq12"

lazy val ItTest = config("it") extend Test

// Test to use self-download or self-build JDBC driver
// unmanagedJars in Compile += file(s"lib/snowflake-jdbc-3.12.12.jar")

lazy val root = project.withId("spark-snowflake").in(file("."))
  .configs(ItTest)
  .settings(inConfig(ItTest)(Defaults.testSettings))
  .settings(Defaults.coreDefaultSettings)
  .settings(Defaults.itSettings)
  .enablePlugins(PublishToArtifactory)
  .settings(
    name := "spark-snowflake",
    organization := "net.snowflake",
    version := s"${sparkConnectorVersion}-spark_3.3",
    scalaVersion := sys.props.getOrElse("SPARK_SCALA_VERSION", default = defaultScalaVersion),
    // Spark 3.2/3.3 supports scala 2.12 and 2.13
    crossScalaVersions := Seq(defaultScalaVersion),
    javacOptions ++= Seq("-source", "17", "-target", "17"),
    javaOptions ++= Seq(
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.math=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
    ),
    scalacOptions ++= Seq("-release", "17"),
    licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0"),
    credentials += Credentials(Path.userHome / ".ivy2" / ".credentials"),
    resolvers ++= Seq(
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "aiq-artifacts".at("s3://s3-us-east-1.amazonaws.com/aiq-artifacts/releases"),
      "Artifactory".at("https://actioniq.jfrog.io/artifactory/aiq-sbt-local/"),
      DefaultMavenRepository,
      Resolver.mavenLocal,
    ),
    libraryDependencies ++= Seq(
      "net.snowflake" % "snowflake-ingest-sdk" % "0.10.8",
      "net.snowflake" % "snowflake-jdbc" % "3.13.30",
      "org.scalatest" %% "scalatest" % "3.1.1" % Test,
      "org.mockito" % "mockito-core" % "1.10.19" % Test,
      "org.apache.commons" % "commons-lang3" % "3.5" % "provided",
      "com.google.guava" % "guava" % "20.0" % "provided",
      // For test to read/write from postgresql
      "org.postgresql" % "postgresql" % "42.5.4" % Test,
      // Below is for Spark Streaming from Kafka test only
      // "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0",
      "org.apache.spark" %% "spark-core" % testSparkVersion % "provided, test",
      "org.apache.spark" %% "spark-sql" % testSparkVersion % "provided, test",
      "org.apache.spark" %% "spark-catalyst" % testSparkVersion % "provided, test",
      "org.apache.spark" %% "spark-mllib" % testSparkVersion % "test",
      "org.apache.spark" %% "spark-core" % testSparkVersion % "provided, test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % testSparkVersion % "provided, test" classifier "tests",
      "org.apache.spark" %% "spark-catalyst" % testSparkVersion %
        "provided, test" classifier "tests",
      "org.apache.spark" %% "spark-core" % testSparkVersion % "provided, test"
        classifier "test-sources",
      "org.apache.spark" %% "spark-sql" % testSparkVersion % "provided, test"
        classifier "test-sources",
      "org.apache.spark" %% "spark-catalyst" % testSparkVersion % "provided, test"
        classifier "test-sources"
      // "org.apache.spark" %% "spark-hive" % testSparkVersion % "provided, test"
    ),

    Test / testOptions += Tests.Argument("-oF"),
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Xms1024M", "-Xmx4096M",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),

    publishMavenStyle := true,
    releaseCrossBuild := true,
    licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,

    pomExtra :=
      <url>https://github.com/snowflakedb/spark-snowflake</url>
        <scm>
          <url>git@github.com:snowflakedb/spark-snowflake.git</url>
          <connection>scm:git:git@github.com:snowflakedb/spark-snowflake.git</connection>
        </scm>
        <developers>
          <developer>
            <id>MarcinZukowski</id>
            <name>Marcin Zukowski</name>
            <url>https://github.com/MarcinZukowski</url>
          </developer>
          <developer>
            <id>etduwx</id>
            <name>Edward Ma</name>
            <url>https://github.com/etduwx</url>
          </developer>
          <developer>
            <id>binglihub</id>
            <name>Bing Li</name>
            <url>https://github.com/binglihub</url>
          </developer>
          <developer>
            <id>Mingli-Rui</id>
            <name>Mingli Rui</name>
            <url>https://github.com/Mingli-Rui</url>
          </developer>
        </developers>,
  )
