name := "LuciusAPI"

version := "1.4"

scalaVersion := "2.10.6"

resolvers += "Job Server Bintray" at "https://dl.bintray.com/spark-jobserver/maven"

resolvers += "bintray-tverbeiren" at "http://dl.bintray.com/tverbeiren/maven"

libraryDependencies += "com.data-intuitive" %% "luciuscore" % "2.0.0"

libraryDependencies += "spark.jobserver" %% "job-server-api" % "0.6.2" % "provided"

libraryDependencies += "spark.jobserver" %% "job-server-extras" % "0.6.2" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1" % "provided"

libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.0"

