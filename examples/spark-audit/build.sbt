scalaVersion := "3.3.8"
resolvers += "okay-effects" at "https://okay-effects.github.io/okay-simple/maven"
libraryDependencies ++= Seq(
  "io.github.okay-effects" %% "okay-effects" % "0.1.0",
  "io.github.okay-effects" %% "okay-effects-machine" % "0.1.0",
  ("org.apache.spark" %% "spark-sql" % "4.2.0").cross(CrossVersion.for3Use2_13))
fork := true
run / javaHome := Some(file(System.getProperty("user.home")) / ".sdkman" / "candidates" / "java" / "25.0.4.1-tem")
run / javaOptions ++= Seq("-Xmx2g") ++ Seq("java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio",
  "java.util", "java.util.concurrent", "java.util.concurrent.atomic", "sun.nio.ch", "sun.nio.cs", "sun.security.action",
  "sun.util.calendar").map(p => s"--add-opens=java.base/$p=ALL-UNNAMED")
