package com.dataintuitive.luciusapi

import com.dataintuitive.test.BaseSparkContextSpec
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD
import org.scalatest.{FunSpec, Matchers}
import spark.jobserver.SparkJobValid
import com.dataintuitive.luciusapi.Common._

/**
  * Tests for the `preprocess` endpoint.
  *
  * TODO: Find a more general approach to this, where a combination of tests is performed automatically.
  */
class initializeTest extends FunSpec with BaseSparkContextSpec with Matchers {

  // Init

  val baseConfig = ConfigFactory.load()

  describe("runJob") {

    val configBlob =
      """
      | {
      |   location = "src/test/resources/processed/"
      |   geneAnnotations = "geneAnnotations.txt"
      | }
    """.stripMargin

    val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

    it("should validate") {

      initialize.validate(sc, thisConfig) should be (SparkJobValid)

    }

    it("should run and return correct result") {

      val runJobResult = initialize.runJob(sc, thisConfig)

    }

    it("The records and gene table should be properly cached/broadcasted") {

      // Collect genes using Common func
      val genes = retrieveGenes(sc, initialize)
      val firstGene = genes.value.genes.head

      firstGene.probesetid should be ("200814_at")

      // Collect the first element from the persisted/named RDD
      val db = retrieveDb(sc, initialize)
      val firstDbElement = db.first

      // Check if the first element is defined
      firstDbElement.pwid.isDefined should be (true)

      }

    }
}
