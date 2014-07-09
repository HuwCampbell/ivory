import sbt._
import Keys._

object depend {
  val scalaz    = Seq("org.scalaz"           %% "scalaz-core"     % "7.0.6",
                      "org.scalaz"           %% "scalaz-effect"   % "7.0.6")

  val scopt     = Seq("com.github.scopt"     %% "scopt"           % "3.2.0")

  val joda      = Seq("joda-time"            %  "joda-time"       % "2.1")

  val specs2    = Seq("specs2-core", "specs2-junit", "specs2-html", "specs2-matcher-extra", "specs2-scalacheck").map(c =>
                      "org.specs2"           %% c                 % "2.3.12" % "test" excludeAll(
    ExclusionRule(organization = "org.scalamacros")
  ))

  val thrift    = Seq("org.apache.thrift"    %  "libthrift"       % "0.9.1" excludeAll(
    ExclusionRule(organization = "org.apache.httpcomponents")
  ))

  val saws      = Seq("com.ambiata"          %% "saws"            % "1.2.1-20140706225344-aad4179" excludeAll(
    ExclusionRule(organization = "org.specs2"),
    ExclusionRule(organization = "javax.mail"),
    ExclusionRule(organization = "com.owtelse.codec"),
    ExclusionRule(organization = "com.ambiata", name = "mundane-testing_2.10")
  ))

  val mundane   = Seq("mundane-io", "mundane-control", "mundane-parse", "mundane-store").map(c =>
                      "com.ambiata"          %% c                 % "1.2.1-20140706115053-2c11cc2") ++
                  Seq("com.ambiata"          %% "mundane-testing" % "1.2.1-20140706115053-2c11cc2" % "test")

  val caliper   = Seq("com.google.caliper"   %  "caliper"         % "0.5-rc1")

  def scoobi(version: String) =
    (if (version.contains("cdh4")) Seq(
      "com.nicta" %% "scoobi" % "0.9.0-cdh4-20140610022328-7b2296d" intransitive(),
      "com.nicta" %% "scoobi-compatibility-cdh4" % "1.0.2" intransitive())
    else if (version.contains("cdh5")) Seq(
      "com.nicta" %% "scoobi" % "0.9.0-cdh5-20140610021710-7b2296d" intransitive(),
      "com.nicta" %% "scoobi-compatibility-cdh5" % "1.0.2" intransitive())
    else
      sys.error(s"unsupported scoobi version, can not build for $version")
    ) ++ Seq(
      "com.thoughtworks.xstream" % "xstream" % "1.4.4" intransitive(),
      "javassist" %  "javassist" % "3.12.1.GA") ++ hadoop(version)


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
