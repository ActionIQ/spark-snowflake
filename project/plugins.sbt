addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.2.1")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.11")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.5")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

// Custom version of fm-sbt-s3-resolver published to our Artifactory for DEVX-275
// See repo here: https://github.com/ActionIQ/sbt-s3-resolver
addSbtPlugin("co.actioniq" % "sbt-s3-resolver" % "1.0.1")