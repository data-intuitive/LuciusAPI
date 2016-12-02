package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD

/**
  * Base trait for the core functionality of the API.
  *
  * Every endpoint should have a function object attached to it that
  * extends this trait and implements its functions. These functions
  * can be used from the endpoints itself (Spark Jobserver) or from a
  * notebook by importing the functions themselves.
  */
trait Functions {

  type Input
  type Parameters
  type Output

  val helpMsg:String

  def help:String = helpMsg

  def info(data:Input, par:Parameters):String

  def header(data:Input, par:Parameters):String

  def result(data:Input, par:Parameters):Output

}
