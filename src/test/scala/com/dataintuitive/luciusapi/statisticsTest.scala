package com.dataintuitive.luciusapi

import com.dataintuitive.test.InitBefore
import com.typesafe.config.ConfigFactory
import org.scalatest.{FunSpec, Matchers}
import spark.jobserver.SparkJobValid

/**
  * Created by toni on 07/10/16.
  */
/*
class statisticsTest extends FunSpec with Matchers with InitBefore {

  import statistics._

  val baseConfig = ConfigFactory.load()

  describe("runJob") {

    val configBlob =
      """
        | {
        | }
      """.stripMargin

    val thisConfig = ConfigFactory.parseString(configBlob).withFallback(baseConfig)

    it("Should validate properly") {
      statistics.validate(sc, thisConfig) should be (SparkJobValid)
    }

    it("Should return the correct result") {
      val expectedResultData:OutputData = Seq(("samples",41774), ("genes", 20336), ("compounds", 41132))

      val result = statistics.runJob(sc, thisConfig).asInstanceOf[Output]
      val outputData = result("data").asInstanceOf[OutputData]

      outputData should be (expectedResultData)
    }

  }
}
*/