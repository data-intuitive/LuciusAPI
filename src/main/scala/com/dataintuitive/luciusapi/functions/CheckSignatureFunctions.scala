package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.utilities.SignedString
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.Map

/**
  * Created by toni on 29/11/16.
  */
object CheckSignatureFunctions extends SessionFunctions {

  case class JobData(db: Dataset[DbRow], genes: Genes, version: String, signature: List[String])

  type JobOutput = List[Map[String, Any]]

  val helpMsg =
    s"""Returns annotations about genes (exists in l1000, symbol).
        |
     |Input:
        |- __`query`__: a gene signature where genes can be in any format symbol, ensembl, probeset, entrez (mandatory)
     """.stripMargin

  def info(data: JobData) = s"Check which genes are contained in L1000 from ${data.signature}"

  def header(data: JobData) =
    s"query (original query gene), inL1000? (if contained), symbol(if in L1000 the symbol it translates to)"

  def result(data: JobData)(implicit sparkSession: SparkSession): JobOutput = {

    implicit def signString(string: String) = new SignedString(string)

    val JobData(db, genes, version, rawSignature) = data

    // Create dictionary based on l1000 genes
    val probesetid2symbol = genes.genes.map(x => (x.probesetid, x.symbol)).toMap
    val ensemblid2symbol = genes.genes.map(x => (x.ensemblid, x.symbol)).toMap
    val entrezid2symbol = genes.genes.map(x => (x.entrezid, x.symbol)).toMap
    val symbol2symbol = genes.genes.map(x => (x.symbol, x.symbol)).toMap

    val tt = probesetid2symbol ++ ensemblid2symbol ++ entrezid2symbol ++ symbol2symbol

    val l1000OrNot = rawSignature
      .map(gene => (gene, tt.get(gene.abs)))
      .map {
        case (gene, optionTranslation) =>
          (gene, optionTranslation.isDefined, tt.getOrElse(gene.abs, ""))
      }

    l1000OrNot.map {
      case (query, inL1000, symbol) =>
        Map("query" -> query, "inL1000" -> inL1000, "symbol" -> (query.sign + symbol))
    }

  }

//  def checkSignature = this.result _

}
