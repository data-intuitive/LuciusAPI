name := "LuciusAPI"

import aether.AetherKeys._

version in ThisBuild := "5.0.0-alpha2"

scalaVersion := "2.11.12"

resolvers += Resolver.githubPackages("data-intuitive")
resolvers += "Artifactory" at "https://sparkjobserver.jfrog.io/artifactory/jobserver/"

libraryDependencies ++= Seq(
  "com.data-intuitive" %% "luciuscore"        % "4.0.1-SNAPSHOT",
  "spark.jobserver"    %% "job-server-api"    % "0.11.1"     % "provided",
  "spark.jobserver"    %% "job-server-extras" % "0.11.1"     % "provided",
  "org.scalactic"      %% "scalactic"         % "3.0.7"      % "test"    ,
  "org.scalatest"      %% "scalatest"         % "3.0.7"      % "test"    ,
  "org.apache.spark"   %% "spark-core"        % "2.4.7"      % "provided",
  "org.apache.spark"   %% "spark-sql"         % "2.4.7"      % "provided"
)

test in assembly := {}

organization := "com.data-intuitive"
licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

// publish to github packages
publishTo := Some("GitHub data-intuitive Apache Maven Packages" at "https://maven.pkg.github.com/data-intuitive/luciusapi")
publishMavenStyle := true
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  "tverbeiren",
  System.getenv("GITHUB_TOKEN")
)

// Publish assembly jar as well
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

aetherPackageMain := assembly.value
