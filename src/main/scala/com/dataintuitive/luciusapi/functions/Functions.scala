package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD

/**
  * Created by toni on 30/11/16.
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
