import sbt._
import Keys._

object depend {
  val scalaz    = Seq("org.scalaz"           %% "scalaz-core"     % "7.1.0",
                      "org.scalaz"           %% "scalaz-effect"   % "7.1.0",
                      "org.scalaz"           %% "scalaz-scalacheck-binding" % "7.1.0" % "test")

  val scopt     = Seq("com.github.scopt"     %% "scopt"           % "3.2.0")
  val joda      = Seq("joda-time"            %  "joda-time"       % "2.1")
  val spire     = Seq("org.spire-math"       %% "spire"           % "0.8.2")
  val argonaut  = Seq("io.argonaut"          %% "argonaut"        % "6.0.4")

  val specs2    = Seq("specs2-core", "specs2-junit", "specs2-html", "specs2-matcher-extra", "specs2-scalacheck").map(c =>
                      "org.specs2"           %% c                 % "2.4.5" % "test" excludeAll ExclusionRule(organization = "org.scalamacros"))

  // NOTE: We have a copy of TDeserializer in core that needs to be kept in sync (or removed) when thrift is updated
  val thrift    = Seq("org.apache.thrift"    %  "libthrift"       % "0.9.1" excludeAll ExclusionRule(organization = "org.apache.httpcomponents"))

  val saws      = Seq("com.ambiata"          %% "saws"            % "1.2.1-20141013060029-6b42321" excludeAll(
    ExclusionRule(organization = "org.specs2"),
    ExclusionRule(organization = "javax.mail"),
    ExclusionRule(organization = "com.owtelse.codec"),
    ExclusionRule(organization = "com.ambiata", name = "mundane-testing_2.10"),
    ExclusionRule(organization = "com.ambiata", name = "mundane-testing_2.11")
  ))

  val MUNDANE_VERSION ="1.2.1-20141011082118-4f6471b"
  val mundane   = Seq("mundane-io", "mundane-control", "mundane-parse", "mundane-store").map(c =>
                      "com.ambiata"          %% c                 % MUNDANE_VERSION) ++
                  Seq("com.ambiata"          %% "mundane-testing" % MUNDANE_VERSION % "test")

  def notion(version: String) =
    if (version.contains("cdh4"))
      Seq("com.ambiata" %% "notion-core"     % "0.0.1-cdh4-20141013220612-a77b0b6") ++
      Seq("com.ambiata" %% "notion-testing"  % "0.0.1-cdh4-20141013220612-a77b0b6" % "test") ++
      hadoop(version)
    else if (version.contains("cdh5"))
      Seq("com.ambiata" %% "notion-core"     % "0.0.1-cdh5-20141013082938-a77b0b6") ++
      Seq("com.ambiata" %% "notion-testing"  % "0.0.1-cdh5-20141013082938-a77b0b6" % "test") ++
       hadoop(version)
    else
      sys.error(s"unsupported poacher version, can not build for $version")

  val caliper   = Seq("com.google.caliper"   %  "caliper"         % "0.5-rc1",
                      "com.google.guava"     %  "guava"           % "14.0.1" force())

  // We _need_ 1.6 for running distributed jobs - otherwise libthrift brings in 1.5 and things break
  val slf4j     = Seq("org.slf4j"            % "slf4j-api"        % "1.6.4")

  def scoobi(version: String) = {
    val jars =
      if (version.contains("cdh4"))      Seq("com.nicta" %% "scoobi"                    % "0.9.0-cdh4-20140925051744-4b8dbfd",
                                             "com.nicta" %% "scoobi-compatibility-cdh4" % "1.0.2")
      else if (version.contains("cdh5")) Seq("com.nicta" %% "scoobi"                    % "0.9.0-cdh5-20140925051050-4b8dbfd",
                                             "com.nicta" %% "scoobi-compatibility-cdh5" % "1.0.2")
      else                               sys.error(s"unsupported scoobi version, can not build for $version")
    jars.map(_ intransitive()) ++ Seq(
      "com.thoughtworks.xstream" % "xstream" % "1.4.4" intransitive(),
      "javassist" %  "javassist" % "3.12.1.GA") ++ hadoop(version)
  }

  def poacher(version: String) =
  if (version.contains("cdh4"))      Seq("com.ambiata" %% "poacher" % "1.0.0-cdh4-20141010000015-2605acf")
  else if (version.contains("cdh5")) Seq("com.ambiata" %% "poacher" % "1.0.0-cdh5-20141009235022-2605acf")
  else                               sys.error(s"unsupported poacher version, can not build for $version")


  def reflect(version: String) =
    Seq("org.scala-lang" % "scala-compiler" % version, "org.scala-lang" % "scala-reflect" % version) ++
      (if (version.contains("2.10")) Seq("org.scalamacros" %% "quasiquotes" % "2.0.0") else Seq())

  def hadoop(version: String, hadoopVersion: String = "2.2.0") =
    if (version.contains("cdh4")) Seq("org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.6.0" % "provided" exclude("asm", "asm"),
                                      "org.apache.hadoop" % "hadoop-core"   % "2.0.0-mr1-cdh4.6.0" % "provided",
                                      "org.apache.avro"   % "avro-mapred"   % "1.7.4" % "provided" classifier "hadoop2")

    else if (version.contains("cdh5")) Seq("org.apache.hadoop" % "hadoop-client" % "2.2.0-cdh5.0.0-beta-2" % "provided" exclude("asm", "asm"),
                                           "org.apache.avro"   % "avro-mapred"   % "1.7.5-cdh5.0.0-beta-2" % "provided")

    else sys.error(s"unsupported hadoop version, can not build for $version")

  val resolvers = Seq(
      Resolver.sonatypeRepo("releases"),
      Resolver.sonatypeRepo("snapshots"),
      Resolver.sonatypeRepo("public"),
      Resolver.typesafeRepo("releases"),
      "cloudera"             at "https://repository.cloudera.com/content/repositories/releases",
      "cloudera2"            at "https://repository.cloudera.com/artifactory/public",
      Resolver.url("ambiata-oss", new URL("https://ambiata-oss.s3.amazonaws.com"))(Resolver.ivyStylePatterns),
      "Scalaz Bintray Repo"  at "http://dl.bintray.com/scalaz/releases")
}
