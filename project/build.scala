import com.ambiata.promulgate.project.ProjectPlugin._
import com.typesafe.sbt.SbtNativePackager._, NativePackagerKeys._
import com.typesafe.tools.mima.plugin.MimaPlugin._
import com.typesafe.tools.mima.plugin.MimaKeys._
import com.typesafe.sbt.SbtProguard._

import sbt._, Keys._, KeyRanks._
import sbtassembly.Plugin._, AssemblyKeys._
import scoverage.ScoverageSbtPlugin._

object build extends Build {
  type Settings = Def.Setting[_]

  lazy val ivory = Project(
    id = "ivory"
  , base = file(".")
  , settings = standardSettings ++ promulgate.library(s"com.ambiata.ivory", "ambiata-oss")
  /** this should aggregate _all_ the projects */
  , aggregate = Seq(
      api
    , benchmark
    , cli
    , core
    , mr
    , operation
    , performance
    , storage
    , testing
    )
  )
  /** this should only ever export _api_, DO NOT add things to this list */
  .dependsOn(api)

  lazy val standardSettings = Defaults.coreDefaultSettings ++
                              projectSettings              ++
                              compilationSettings          ++
                              testingSettings              ++
                              Seq[Settings](
                                resolvers := depend.resolvers
                              )

  lazy val projectSettings: Seq[Settings] = Seq(
    name := "ivory"
  , version in ThisBuild := s"""1.0.0-${Option(System.getenv("HADOOP_VERSION")).getOrElse("cdh5")}"""
  , organization := "com.ambiata"
  , scalaVersion := "2.11.4"
  , crossScalaVersions := Seq(scalaVersion.value)
  , fork in run  := true
  , publishArtifact in packageDoc := false
  , publishArtifact in (Test, packageBin) := true //publishArtifact in Test := true
  /* https://gist.github.com/djspiewak/976cd8ac65e20e136f05 */
  , unmanagedSourceDirectories in Compile += (sourceDirectory in Compile).value / s"scala-${scalaBinaryVersion.value}"
  , updateOptions := updateOptions.value.withCachedResolution(true)
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
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.hadoop(version.value) ++ depend.poacher(version.value) ++ depend.slf4j
    )
  )
  .dependsOn(operation)

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
    , jarTest <<= assembly.map({ case p => testJarSize(p) })
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
    ) ++ Seq[Settings](libraryDependencies ++= depend.pirate ++ depend.scalaz ++ depend.hadoop(version.value)  ++ depend.poacher(version.value) ++ depend.specs2 ++ depend.slf4j)
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
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.mundane ++ depend.joda ++ depend.specs2 ++ depend.argonaut ++
                                               depend.thrift ++ depend.hadoop(version.value) ++ depend.reflect(scalaVersion.value) ++
                                               depend.scoobi(version.value) ++ depend.poacher(version.value) ++
                                               depend.saws ++ depend.notion(version.value) ++ depend.disorder)
  )

  lazy val operation = Project(
    id = "operation"
  , base = file("ivory-operation")
  , settings = standardSettings ++ lib("operation") ++ Seq[Settings](
      name := "ivory-operation"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.joda ++ depend.hadoop(version.value) ++
      depend.specs2 ++ depend.mundane ++ depend.poacher(version.value) ++ depend.spire ++ depend.argonaut ++ depend.base64 ++ depend.disorder)
  )
  .dependsOn(core, storage, mr % "test->test", core % "test->test", storage % "test->test")

  lazy val mr = Project(
    id = "mr"
  , base = file("ivory-mr")
  , settings = standardSettings ++ lib("mr") ++ Seq[Settings](
      name := "ivory-mr"
    ) ++ Seq[Settings](libraryDependencies ++= depend.thrift ++ depend.mundane ++ depend.scalaz ++ depend.specs2 ++ depend.poacher(version.value) ++ depend.hadoop(version.value))
  )
  .dependsOn(core, core % "test->test")

  lazy val performance = Project(
    id = "performance"
    , base = file("ivory-performance")
    , settings = standardSettings ++ app("performance") ++ Seq[Settings](
      name := "ivory-performance"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.mundane)
  )
    .dependsOn(core)

  lazy val storage = Project(
    id = "storage"
  , base = file("ivory-storage")
  , settings = standardSettings ++ lib("storage") ++ Seq[Settings](
      name := "ivory-storage"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.mundane ++ depend.scoobi(version.value) ++
                                               depend.argonaut ++ depend.poacher(version.value) ++ depend.specs2 ++
                                               depend.saws ++ depend.disorder)
  )
  .dependsOn(core, mr, core % "test->test", mr % "test->test")

  lazy val testing = Project(
      id = "testing"
    , base = file("ivory-testing")
    , settings = standardSettings ++ lib("testing") ++ Seq[Settings](
      name := "ivory-testing"
    ) ++ Seq[Settings](libraryDependencies ++= depend.scalaz ++ depend.mundane ++ depend.scoobi(version.value) ++ depend.poacher(version.value) ++ depend.specs2 ++ depend.saws ++ depend.notion(version.value) ++ depend.hadoop(version.value))
  ).dependsOn(core % "compile->test", operation % "compile->test")

  lazy val compilationSettings: Seq[Settings] = Seq(
    javaOptions ++= Seq("-Xmx3G", "-Xms512m", "-Xss4m")
  , javacOptions ++= Seq("-source", "1.6", "-target", "1.6")
  , maxErrors := 10
  , scalacOptions in Compile ++= Seq(
      "-target:jvm-1.6"
    , "-deprecation"
    , "-unchecked"
    , "-feature"
    , "-language:_"
    , "-Ywarn-value-discard"
    , "-Yno-adapted-args"
    , "-Xlint"
    , "-Xfatal-warnings"
    , "-Yinline-warnings"
    ) ++ importWarnings(scalaBinaryVersion.value)
  , scalacOptions in (Compile,doc) := Seq("-language:_", "-feature")
  , scalacOptions in (Compile,console) := Seq("-language:_", "-feature")
  , scalacOptions in (Test,console) := Seq("-language:_", "-feature")
  )

  def importWarnings(version: String) =
    if (version == "2.11")
      Seq(
        "-Ywarn-unused-import"
      // , "-Ywarn-unused" -- this would be nice, but breaks on synthetics from pattern matching, retry with 2.11.3
      )
    else
      Seq()

  lazy val testingSettings: Seq[Settings] = Seq(
    logBuffered := false
  , cancelable := true
  , fork in Test := Option(System.getenv("NO_FORK")).map(_ != "true").getOrElse(true)
  , javaOptions in Test ++= Seq("-Dfile.encoding=UTF8", "-XX:MaxPermSize=512m", "-Xms512m", "-Xmx2g", "-XX:+CMSClassUnloadingEnabled", "-XX:+UseConcMarkSweepGC")
  , testOptions in Test += Tests.Setup(() => System.setProperty("log4j.configuration", "file:etc/log4j-test.properties"))
  , testOptions in Test += Tests.Argument(TestFrameworks.Specs2, "tracefilter", "/.*specs2.*,.*mundane.testing.*")
  , testOptions in Test ++= Seq(Tests.Argument("--"))
  , testOptions in Test ++= {
    val aws = Option(System.getenv("FORCE_AWS")).isDefined || Option(System.getenv("AWS_ACCESS_KEY")).isDefined
    val mr = !Option(System.getenv("NO_MR")).isDefined
    val excludes = List("aws").filter(_ => !aws) ++ List("mr").filter(_ => !mr)
    if (excludes.isEmpty)
      Seq()
    else
      Seq(Tests.Argument("exclude", excludes.mkString(",")))
  })

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
    , base / ".." / "etc" / "thrift-NOTICE.txt" -> "thrift-NOTICE.txt"
    , base / ".." / "ivory-operation" / "src" / "main" / "thrift" / "ingest.thrift" -> "ivory.thrift"
    ) })

  lazy val jarTest = taskKey[Unit]("Test jar size")

  def testJarSize(pack: File) = {
    val p = pack.getAbsolutePath
    val s = s"jar tvf $p"
    println(s" Running $s")
    val i = (s #| "wc -l" !!).trim.toInt
    val max = 65536
    if (i > max)
      sys.error(s"Jar is too fat! Number of files in jar is $i, needs to be less than $max")
    else
      println(s"Jar size is ok! Number of files in jar is $i (max: $max)")
  }
}
