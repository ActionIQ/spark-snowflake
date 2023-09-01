addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.1")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.11")

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.3")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.2")

// Custom version of fm-sbt-s3-resolver published to our Artifactory for DEVX-275
// See repo here: https://github.com/ActionIQ/sbt-s3-resolver
addSbtPlugin("co.actioniq" % "sbt-s3-resolver" % "1.0.1")