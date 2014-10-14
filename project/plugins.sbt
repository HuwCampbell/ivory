scalacOptions += "-deprecation"

resolvers += Resolver.typesafeRepo("releases")

resolvers += Resolver.sonatypeRepo("releases")

resolvers += "Era7 maven releases" at "http://releases.era7.com.s3.amazonaws.com"

resolvers += Resolver.url("ambiata-oss", new URL("https://ambiata-oss.s3.amazonaws.com"))(Resolver.ivyStylePatterns)

resolvers += Resolver.sonatypeRepo("releases")

addSbtPlugin("com.ambiata" % "promulgate" % "0.11.0-20141014013725-80c129f")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "0.7.1")

addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "0.1.6")

addSbtPlugin("com.typesafe.sbt" % "sbt-proguard" % "0.2.2" )

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "0.99.7.1")
