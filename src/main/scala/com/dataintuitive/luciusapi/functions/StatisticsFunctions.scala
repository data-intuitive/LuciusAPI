package com.dataintuitive.luciusapi.functions

import org.apache.spark.rdd.RDD
import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import scala.collection.immutable.Map

object StatisticsFunctions extends Functions {

  type Input = (RDD[DbRow], Genes)
  type Parameters = Null
  type Output = Map[String, Any]

  def info(data:Input, par:Parameters) = "General statistics about the dataset"

  def header(data:Input, par:Parameters) = Map("key" -> "value").toString

  def result(data:Input, par:Parameters) = {

    val (db, genes) = data

    Map("samples"   -> db.count,
        "genes"     -> genes.genes.length,
        "compounds" -> db.map(_.compoundAnnotations.compound.name).distinct.count
    )
  }

  def statistics = result _

}