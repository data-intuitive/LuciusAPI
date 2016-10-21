package com.dataintuitive.luciusapi

import com.dataintuitive.test.InitBefore
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}
import spark.jobserver.{SparkJobInvalid, SparkJobValid}

/**
  * Tests for the `preprocess` endpoint.
  *
  * TODO: Find a more general approach to this, where a combination of tests is performed automatically.
  */
class checkSignatureTest extends FunSpec with Matchers with InitBefore {

  import checkSignature._

  // Init

  val baseConfig = ConfigFactory.load()

  describe("checkSignature") {

    it("Should return help message when asked for - 'help=true'") {

      val configBlob =
        """
          | { help = true
          | }
        """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      validate(sc, thisConfig) should be(SparkJobInvalid(helpMsg))
    }

    it("Should return the correct result") {

      // v2 interface
      val configBlob =
      """
          | {
          |   query = MELK BRCA1 NotKnown 00000_at 200814_at
          | }
        """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      val expectedResult:OutputData = Seq(("MELK",true, "MELK"), ("BRCA1",true, "BRCA1"), ("NotKnown",false, ""), ("00000_at",false, ""), ("200814_at",true, "PSME1"))

      validate(sc,thisConfig) should be (SparkJobValid)

      val result = runJob(sc, thisConfig).asInstanceOf[Output]
      val resultData = result("data").asInstanceOf[OutputData]

      resultData should be (expectedResult)
    }

  }
}
