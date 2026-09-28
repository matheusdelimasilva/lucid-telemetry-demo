ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-hello",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.9",
      "org.apache.spark" %% "spark-protobuf" % "3.5.9",
      "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
    ),
    Compile / PB.protoSources := Seq(baseDirectory.value / "proto"),
    Compile / PB.protocVersion := "3.25.5",
    Compile / PB.targets := Seq(
      scalapb.gen() -> (Compile / sourceManaged).value
    ),
    Compile / run / mainClass := Some("SmokeHello"),
    run / fork := true,
    run / javaOptions ++= Seq(
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
    ),
    run / connectInput := true
  )
