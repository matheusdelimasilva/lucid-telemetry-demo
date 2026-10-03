ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.9"
val javaOpens = Seq(
  "-Xmx1g",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
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
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

val commonSettings = Seq(
  run / fork := true,
  run / javaOptions ++= javaOpens,
  run / connectInput := true,
  Test / fork := true,
  Test / javaOptions ++= javaOpens,
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
)

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-protobuf" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

lazy val common = (project in file("common"))
  .settings(commonSettings)
  .settings(
    name := "legacy-common",
    libraryDependencies ++= sparkDependencies
  )

lazy val chargingSessions = (project in file("charging-sessions"))
  .dependsOn(common)
  .settings(commonSettings)
  .settings(
    name := "charging-sessions",
    Compile / run / mainClass := Some("legacy.charging.ChargingSessions")
  )

lazy val batteryHealth = (project in file("battery-health"))
  .dependsOn(common)
  .settings(commonSettings)
  .settings(
    name := "battery-health",
    Compile / run / mainClass := Some("legacy.battery.BatteryHealth")
  )

lazy val replayHarness = (project in file("replay-harness"))
  .dependsOn(common, chargingSessions, batteryHealth)
  .settings(commonSettings)
  .settings(
    name := "replay-harness",
    Compile / run / mainClass := Some("legacy.replay.Harness"),
    libraryDependencies ++= Seq(
      "com.google.protobuf" % "protobuf-java-util" % "3.25.5",
      "com.google.protobuf" % "protobuf-java" % "3.25.5"
    )
  )

lazy val root = (project in file("."))
  .aggregate(common, chargingSessions, batteryHealth, replayHarness)
  .settings(
    commonSettings,
    name := "legacy-spark"
  )
