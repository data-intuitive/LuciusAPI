package com.dataintuitive.luciusapi

import com.dataintuitive.test.InitBefore
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}

class topTableTest extends FunSpec with Matchers with InitBefore {

  import topTable._

  // Init

  val baseConfig = ConfigFactory.load()

  describe("topTable validate") {

    it("Should return help message when asked for - 'help=true'") {

      val configBlob =
        """
          | { help = true
          | }
        """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      //validate(sc, thisConfig) should be (SparkJobInvalid(helpMsg))
      val jobData = topTable.validate(sparkSession, runtime, thisConfig)
      jobData.isGood shouldBe false
    }

    it("Should validate correctly") {

      // v2 interface
      val configBlob =
        """
          |{
          |  version = v2,
          |  features = zhang jnjs id smiles,
          |  head = 14,
          |  query= HSPA1A DNAJB1 BAG3 P4HA2 HSPA8 TMEM97 SPR DDIT4 HMOX1 -TSEN2
          |}
        """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      val jobData = topTable.validate(sparkSession, runtime, thisConfig)

      jobData.isGood shouldBe true
      jobData.get.version shouldBe "v2"
      jobData.get.specificData.head shouldBe 14
      jobData.get.specificData.tail shouldBe 0
      jobData.get.specificData.signatureQuery shouldBe List("HSPA1A", "DNAJB1", "BAG3", "P4HA2", "HSPA8", "TMEM97", "SPR", "DDIT4", "HMOX1", "-TSEN2")
      jobData.get.specificData.featuresQuery shouldBe List("zhang", "jnjs", "id", "smiles")
      jobData.get.specificData.filters shouldBe List()
    }

    it("Should not validate if the head parameter is set to 'yes'") {

      // v2 interface
      val configBlob =
        """
          |{
          |  version = v2,
          |  features = zhang jnjs id smiles,
          |  head = yes,
          |  query= HSPA1A DNAJB1 BAG3 P4HA2 HSPA8 TMEM97 SPR DDIT4 HMOX1 -TSEN2
          |}
        """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      val jobData = topTable.validate(sparkSession, runtime, thisConfig)

      jobData.isGood shouldBe false

    }

    it("Should not allow both head and tail") {

      // v2 interface
      val configBlob =
        """
          |{
          |  version = v2,
          |  features = zhang jnjs id smiles,
          |  head = 14,
          |  tail = 5
          |  query= HSPA1A DNAJB1 BAG3 P4HA2 HSPA8 TMEM97 SPR DDIT4 HMOX1 -TSEN2
          |}
        """.stripMargin

      val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

      val jobData = topTable.validate(sparkSession, runtime, thisConfig)

      jobData.isGood shouldBe false

    }

  }

}
