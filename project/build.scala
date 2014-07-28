import com.ambiata.promulgate.project.ProjectPlugin._
import com.typesafe.sbt.SbtNativePackager._, NativePackagerKeys._
import com.typesafe.tools.mima.plugin.MimaPlugin._
import com.typesafe.tools.mima.plugin.MimaKeys._
import com.typesafe.sbt.SbtProguard._

import sbt._, Keys._, KeyRanks._
import sbtassembly.Plugin._, AssemblyKeys._


object build extends Build {
  type Settings = Def.Setting[_]

  lazy val ivory = Project(
    id = "ivory"
  , base = file(".")
  , settings = standardSettings ++ promulgate.library(s"com.ambiata.ivory", "ambiata-oss")
  /* this should aggregate _all_ the projects */
  , aggregate = Seq(
      api
    , benchmark
    , cli
    , core
    , data
    , extract
    , generate
    , ingest
    , mr
    , scoobi
    , storage
    , tools
    , alien_hdfs
    )
  )
  /* this should only ever export _api_, DO NOT add things to this list */
  .dependsOn(api)

  lazy val standardSettings = Defaults.defaultSettings ++
                              projectSettings          ++
                              compilationSettings      ++
                              testingSettings          ++
                              Seq[Settings](
                                resolvers := depend.resolvers
                              )

  lazy val projectSettings: Seq[Settings] = Seq(
    name := "ivory"
  , version in ThisBuild := s"""1.0.0-${Option(System.getenv("HADOOP_VERSION")).getOrElse("cdh5")}"""
  , organization := "com.ambiata"
  , scalaVersion := "2.11.2"
  , crossScalaVersions := Seq("2.10.4", scalaVersion.value)
  , fork in run  := true
  , publishArtifact in packageDoc := false
  // https://gist.github.com/djspiewak/976cd8ac65e20e136f05
  , unmanagedSourceDirectories in Compile += (sourceDirectory in Compile).value / s"scala-${scalaBinaryVersion.value}"
  ) ++ Seq(prompt)

  def lib(name: String) =
    promulgate.library(s"com.ambiata.ivory.$name", "ambiata-oss")

  def app(name: String) =
    promulgate.all(s"com.ambiata.ivory.$name", "ambiata-oss", "ambiata-dist")

  lazy val api = Project(
    id = "api"
  , base = file("ivory-api")
  , settings = standardSettings ++ lib("api") ++ mimaDefaultSettings ++ Seq[Settings](
      name := "ivory-api"
    , previousArtifact := Some("com.ambiata" %% "ivory-api" % "1.0.0-cdh5-20140703185823-2efc9c3")
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.scoobi(version.value) ++ depend.slf4j)
  )
  .dependsOn(generate, ingest, tools, extract)

  lazy val benchmark = Project(
    id = "benchmark"
  , base = file("ivory-benchmark")
  , settings = standardSettings ++ app("benchmark") ++ Seq[Settings](
      name := "ivory-benchmark"
    , fork in run := true
    , run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
    , javaOptions in run <++= (fullClasspath in Runtime).map(cp => Seq("-cp", sbt.Attributed.data(cp).mkString(":")))
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.hadoop(version.value) ++ depend.caliper)
  )
  .dependsOn(api)

  lazy val cli = Project(
    id = "cli"
  , base = file("ivory-cli")
  , settings = standardSettings ++ app("cli") ++ universalSettings ++ proguardSettings ++ Seq[Settings](
      name := "ivory-cli"
    , dist
    , mainClass in assembly := Some("com.ambiata.ivory.cli.main")
    , ProguardKeys.inputs in Proguard <<= (assembly).map(f => Seq(f))
    , ProguardKeys.libraries in Proguard := Seq()
    , ProguardKeys.options in Proguard  += """
      -libraryjars <java.home>/lib/rt.jar
      -dontobfuscate
      -dontoptimize
      -dontwarn scala.**
      -dontwarn org.apache.hadoop.**
      -dontwarn org.apache.avro.**
      -dontwarn org.apache.avalon.**
      -dontwarn org.apache.commons.lang.**
      -dontwarn org.specs2.**
      -dontwarn org.scalacheck.**
      -dontwarn org.springframework.**
      -dontwarn com.owtelse.**
      -dontwarn org.jdom.**
      -dontwarn org.junit.**
      -dontwarn org.aspectj.**
      -dontwarn org.slf4j.**
      -dontwarn scodec.**
      -dontwarn org.fusesource.**
      -dontwarn org.apache.log4j.**
      -dontwarn org.apache.log.**
      -dontwarn com.bea.xml.**
      -dontwarn com.nicta.scoobi.**
      -dontwarn com.amazonaws.**
      -dontwarn org.xmlpull.**
      -dontwarn net.sf.cglib.**
      -dontwarn nu.xom.**
      -dontwarn com.ctc.wstx.**
      -dontwarn org.kxml2.**
      -dontwarn org.dom4j.**
      -dontwarn org.codehaus.jettison.**
      -dontwarn javassist.**
      -dontwarn javax.**
      -dontnote **
      -dontskipnonpubliclibraryclasses
      -keep class com.ambiata.ivory.**
      -keepclassmembers class * { ** MODULE$; }
      -keepclassmembers class * { ** serialVersionUID; }
    """
    , javaOptions in (Proguard, ProguardKeys.proguard) := Seq("-Xmx2G")
    ) ++ Seq[Settings](libraryDependencies ++= depend.scopt ++ depend.scalaz ++ depend.scoobi(version.value) ++ depend.specs2 ++ depend.slf4j)
      ++ addArtifact(Artifact("ivory", "dist", "tgz"), packageZipTarball in Universal)
      ++ addArtifact(Artifact("ivory", "dist", "zip"), packageBin in Universal)
  )
  .dependsOn(api)

  lazy val core = Project(
    id = "core"
  , base = file("ivory-core")
  , settings = standardSettings ++ lib("core") ++ Seq[Settings](
      name := "ivory-core"
    , libraryDependencies ++= (if (scalaVersion.value.contains("2.10")) Seq(compilerPlugin("org.scalamacros" %% "paradise" % "2.0.0" cross CrossVersion.full)) else Nil)
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.mundane ++ depend.joda ++ depend.specs2 ++
                                               depend.thrift ++ depend.hadoop(version.value) ++ depend.reflect(scalaVersion.value))
  )
  .dependsOn(data)

  lazy val data = Project(
    id = "data"
  , base = file("ivory-data")
  , settings = standardSettings ++ lib("data") ++ Seq[Settings](
      name := "ivory-data"
    , libraryDependencies ++= (if (scalaVersion.value.contains("2.10")) Seq(compilerPlugin("org.scalamacros" %% "paradise" % "2.0.0" cross CrossVersion.full)) else Nil)
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.mundane ++ depend.specs2 ++
                                               depend.hadoop(version.value) ++ depend.reflect(scalaVersion.value))
  )

  lazy val extract = Project(
    id = "extract"
  , base = file("ivory-extract")
  , settings = standardSettings ++ lib("extract") ++ Seq[Settings](
      name := "ivory-extract"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.scoobi(version.value) ++ depend.specs2 ++ depend.mundane)
  )
  .dependsOn(core, scoobi, storage, mr, core % "test->test", scoobi % "test->test", storage % "test->test")

  lazy val generate = Project(
    id = "generate"
  , base = file("ivory-generate")
  , settings = standardSettings ++ lib("generate") ++ Seq[Settings](
      name := "ivory-generate"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.joda ++ depend.scoobi(version.value) ++ depend.specs2)
  )
  .dependsOn(core, storage)

  lazy val ingest = Project(
    id = "ingest"
  , base = file("ivory-ingest")
  , settings = standardSettings ++ lib("ingest") ++ Seq[Settings](
      name := "ivory-ingest"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.joda ++ depend.specs2 ++ depend.scoobi(version.value) ++ depend.saws ++ depend.mundane)
  )
  .dependsOn(core, storage, alien_hdfs, scoobi, mr, scoobi % "test->test", core % "test->test")

  lazy val mr = Project(
    id = "mr"
  , base = file("ivory-mr")
  , settings = standardSettings ++ lib("mr") ++ Seq[Settings](
      name := "ivory-mr"
    ) ++ Seq[Settings](libraryDependencies ++= depend.thrift ++ depend.mundane ++ depend.scalaz ++ depend.specs2 ++ depend.hadoop(version.value))
  )
  .dependsOn(core, alien_hdfs, core % "test->test")

  lazy val scoobi = Project(
    id = "scoobi"
  , base = file("ivory-scoobi")
  , settings = standardSettings ++ lib("scoobi") ++ Seq[Settings](
      name := "ivory-scoobi"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.scoobi(version.value) ++ depend.saws ++ depend.specs2 ++ depend.mundane)
  )
  .dependsOn(core, alien_hdfs)

  lazy val storage = Project(
    id = "storage"
  , base = file("ivory-storage")
  , settings = standardSettings ++ lib("storage") ++ Seq[Settings](
      name := "ivory-storage"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.scoobi(version.value) ++ depend.specs2 ++ depend.saws)
  )
  .dependsOn(core, data, scoobi, alien_hdfs, mr, core % "test->test",  scoobi % "test->test", data % "test->test")

  lazy val tools = Project(
    id = "tools"
  , base = file("ivory-tools")
  , settings = standardSettings ++ lib("tools") ++ Seq[Settings](
      name := "ivory-tools"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.scoobi(version.value) ++ depend.specs2 ++ depend.mundane)
  )
  .dependsOn(core, extract, scoobi, storage, core % "test->test", storage % "test->test")

  lazy val alien_hdfs = Project(
    id = "hdfs"
  , base = file("ivory-alien-hdfs")
  , settings = standardSettings ++ lib("alien.hdfs") ++ Seq[Settings](
      name := "ivory-alien-hdfs"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.hadoop(version.value) ++ depend.mundane ++ depend.saws ++ depend.scoobi(version.value) ++ depend.specs2)
  )

  lazy val compilationSettings: Seq[Settings] = Seq(
    javaOptions ++= Seq("-Xmx3G", "-Xms512m", "-Xss4m")
  , javacOptions ++= Seq("-source", "1.6", "-target", "1.6")
  , maxErrors := 20
  , scalacOptions ++= Seq("-target:jvm-1.6", "-deprecation", "-unchecked", "-feature", "-language:_", "-Xlint", "-Xfatal-warnings", "-Yinline-warnings")
  )

  lazy val testingSettings: Seq[Settings] = Seq(
    logBuffered := false
  , cancelable := true
  , fork in test := true
  , testOptions in Test += Tests.Setup(() => System.setProperty("log4j.configuration", "file:etc/log4j-test.properties"))
  , javaOptions += "-Xmx3G"
  )

  lazy val prompt = shellPrompt in ThisBuild := { state =>
    val name = Project.extract(state).currentRef.project
    (if (name == "ivory") "" else name) + "> "
  }

  lazy val dist =
    mappings in Universal <<= (baseDirectory, assembly).map({ case (base, jar) => Seq(
      jar -> "lib/ivory.jar"
    , base / "src" / "main"/ "bin" / "ivory" -> "bin/ivory"
    , base / ".." / "NOTICE.txt" -> "NOTICE.txt"
    , base / ".." / "README.md" -> "README.md"
    ) })
}
