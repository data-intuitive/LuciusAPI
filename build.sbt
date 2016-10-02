name := "LuciusAPI"

version := "1.4"

scalaVersion := "2.10.6"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

resolvers += "bintray-tverbeiren" at "http://dl.bintray.com/tverbeiren/maven"

libraryDependencies ++= Seq(
  "com.data-intuitive" %% "luciuscore"        % "2.0.0",
  "spark.jobserver"    %% "job-server-api"    % "0.6.2"     % "provided",
  "spark.jobserver"    %% "job-server-extras" % "0.6.2"     % "provided",
  "org.scalactic"      %% "scalactic"         % "3.0.0"     % "test"    ,
  "org.scalatest"      %% "scalatest"         % "3.0.0"     % "test"    ,
  "org.apache.spark"   %% "spark-core"        % "1.6.2"     % "provided",
  "org.scalaz"         %% "scalaz-core"       % "7.2.0"
)
