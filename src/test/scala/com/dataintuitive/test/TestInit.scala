package com.dataintuitive.test

import com.dataintuitive.luciusapi.initialize
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

object BaseSparkContextSpec {

  lazy val conf = new SparkConf()
    .setAppName("Test")
    .setMaster("local[*]")
  lazy val sc = new SparkContext(conf)

}


trait BaseSparkContextSpec {

  lazy val sc = BaseSparkContextSpec.sc

}

trait InitBefore extends Suite with BaseSparkContextSpec with BeforeAndAfterAll { this: Suite =>

    override def beforeAll() {

      val baseConfig = ConfigFactory.load()

      val configBlob =
        """
      | {
      |   location = "src/test/resources/processed/"
      |   geneAnnotations = "geneAnnotations.txt"
      | }
      """.stripMargin

    val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

    Thread.sleep(5000)

    val runJobResult = initialize.runJob(sc, thisConfig)

    Thread.sleep(5000)

    }

}