name := "LuciusAPI"

version := "3.3.3"

scalaVersion := "2.11.12"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

resolvers += "Local Ivy" at "file://Users/toni/.ivy2/local"

resolvers += Resolver.githubPackages("data-intuitive")

libraryDependencies ++= Seq(
  "com.data-intuitive" %% "luciuscore"        % "3.3.3",
  "spark.jobserver"    %% "job-server-api"    % "0.8.0"      % "provided",
  "spark.jobserver"    %% "job-server-extras" % "0.8.0"      % "provided",
  "org.scalactic"      %% "scalactic"         % "3.0.7"      % "test"    ,
  "org.scalatest"      %% "scalatest"         % "3.0.7"      % "test"    ,
  "org.apache.spark"   %% "spark-core"        % "2.3.1"      % "provided",
  "org.apache.spark"   %% "spark-sql"         % "2.3.1"      % "provided"
  /* "org.scalaz"         %% "scalaz-core"       % "7.2.0" */
)

test in assembly := {}

organization := "com.data-intuitive"
licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))

// Publish assembly jar as well
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}

addArtifact(artifact in (Compile, assembly), assembly)

githubOwner := "data-intuitive"
githubRepository := "luciusapi"
githubTokenSource := TokenSource.GitConfig("github.token")
publishMavenStyle := true
