package com.dataintuitive.luciusapi

import com.dataintuitive.test.BaseSparkContextSpec
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import org.apache.spark._
import preprocess._
import org.scalatest.FlatSpec
import spark.jobserver.{SparkJobInvalid, SparkJobValid, SparkJobValidation}

/**
  * Created by toni on 02/10/16.
  */
class preprocessTest extends FlatSpec with BaseSparkContextSpec{

  // Init

  val baseConfig = ConfigFactory.load()

  val initConfig = baseConfig
                      .withValue("locationFrom", ConfigValueFactory.fromAnyRef("locFrom"))
                      .withValue("locationTo", ConfigValueFactory.fromAnyRef("locTo"))
                      .withValue("AWS_ACCESS_KEY_ID", ConfigValueFactory.fromAnyRef("ACCESS"))
                      .withValue("AWS_SECRET_ACCESS_KEY", ConfigValueFactory.fromAnyRef("SECRET"))

  // Validation
  info("Test validation")

  "Validation" should "return appropriate errors" in {

    assert(preprocess.validate(sc, initConfig) === SparkJobInvalid("AWS Access key not exported in shell, AWS Secret key not exported in shell"))

  }


}
