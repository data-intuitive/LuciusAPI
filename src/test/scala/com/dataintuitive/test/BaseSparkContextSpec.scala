package com.dataintuitive.test

import org.apache.spark.{SparkConf, SparkContext}

object BaseSparkContextSpec {

  lazy val conf = new SparkConf()
    .setAppName("Test")
    .setMaster("local[*]")
  lazy val sc = new SparkContext(conf)

}

trait BaseSparkContextSpec {

  lazy val sc = BaseSparkContextSpec.sc

}
