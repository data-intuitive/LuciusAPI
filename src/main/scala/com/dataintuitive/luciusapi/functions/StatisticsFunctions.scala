package com.dataintuitive.luciusapi.functions

import org.apache.spark.rdd.RDD
import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow

object StatisticsFunctions {

  type Config = (RDD[DbRow], Genes)

  def info(config:Config) = "General statistics about the dataset"

  def header(config:Config) = Map("key" -> "value")

  def statistics(config:Config):Map[String, Any] = {

    val (db, genes) = config

    Map("samples"   -> db.count,
        "genes"     -> genes.genes.length,
        "compounds" -> db.map(_.compoundAnnotations.compound.name).distinct.count
    )
  }

}