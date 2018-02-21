package com.dataintuitive.jobserver

import spark.jobserver._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.Dataset

/**
  * wrapper for named objects of type DataFrame
  */
case class NamedDataSet[T](ds: Dataset[T], forceComputation: Boolean, storageLevel: StorageLevel)
    extends NamedObject

