package com.dataintuitive.luciusapi.functions

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import scala.collection.immutable.Map

object StatisticsFunctions extends SessionFunctions {

  type Input = (Dataset[DbRow], Genes)
  type Parameters = Null
  type Output = Map[String, Any]

  val helpMsg = "Return general statistics about the dataset.\nNo input is required. Pass null for parameters in Scala"

  def info(data:Input, par:Parameters) = "General statistics about the dataset"

  def header(data:Input, par:Parameters) = Map("key" -> "value").toString

  def result(data:Input, par:Parameters)(implicit sparkSession:SparkSession) = {

    import sparkSession.implicits._

    val (db, genes) = data

    val compounds = Map(
        "total" -> db.filter(_.sampleAnnotations.sample.getProtocolname != "").map(_.compoundAnnotations.compound.jnjs).distinct.count,
        "mcf7" -> db.filter(_.sampleAnnotations.sample.getProtocolname == "MCF7").map(_.compoundAnnotations.compound.jnjs).distinct.count,
        "pbmc" -> db.filter(_.sampleAnnotations.sample.getProtocolname == "PBMC").map(_.compoundAnnotations.compound.jnjs).distinct.count
    )

    val samples = Map(
        "total" -> db.filter(_.sampleAnnotations.sample.getProtocolname != "").map(_.pwid).distinct.count,
        "mcf7" -> db.filter(_.sampleAnnotations.sample.getProtocolname == "MCF7").map(_.pwid).distinct.count,
        "pbmc" -> db.filter(_.sampleAnnotations.sample.getProtocolname == "PBMC").map(_.pwid).distinct.count
    )

    val informative = Map(
        "total" -> db.filter(_.sampleAnnotations.sample.getProtocolname != "").filter(sample => sample.sampleAnnotations.p.map(_.count(_ <= 0.05)).getOrElse(0) > 0).count,
        "mcf7" -> db.filter(_.sampleAnnotations.sample.getProtocolname == "MCF7").filter(sample => sample.sampleAnnotations.p.map(_.count(_ <= 0.05)).getOrElse(0) > 0).count,
        "pbmc" -> db.filter(_.sampleAnnotations.sample.getProtocolname == "PBMC").filter(sample => sample.sampleAnnotations.p.map(_.count(_ <= 0.05)).getOrElse(0) > 0).count
    )

    Map("samples"   -> samples,
        "compounds" -> compounds,
        "informative" -> informative
    )
  }

//   def statistics = result _

}