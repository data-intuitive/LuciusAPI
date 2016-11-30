package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.GeneModel.Genes
import com.dataintuitive.luciuscore.Model.DbRow
import org.apache.spark.rdd.RDD
import scala.collection.immutable.Map

/**
  * Created by toni on 29/11/16.
  */
object CheckSignatureFunctions extends Functions {

  type Input = (RDD[DbRow], Genes)
  type Parameters = List[String]
  type Output = List[Map[String, Any]]

  val helpMsg =
    s"""Returns annotations about genes (exists in l1000, symbol).
        |
     |Input:
        |- __`query`__: a gene signature where genes can be in any format symbol, ensembl, probeset, entrez (mandatory)
     """.stripMargin

  def info(data:Input, par:Parameters) = s"Check which genes are contained in L1000 from $par"

  def header(data:Input, par:Parameters) = s"query (original query gene), inL1000? (if contained), symbol(if in L1000 the symbol it translates to)"

  def result(data:Input, par:Parameters):Output = {

    val (db, genes) = data
    val rawSignature = par

    // Create dictionary based on l1000 genes
    val probesetid2symbol = genes.genes.map(x => (x.probesetid, x.symbol)).toMap
    val ensemblid2symbol  = genes.genes.map(x => (x.ensemblid, x.symbol)).toMap
    val entrezid2symbol   = genes.genes.map(x => (x.entrezid, x.symbol)).toMap
    val symbol2symbol   = genes.genes.map(x => (x.symbol, x.symbol)).toMap

    val tt = probesetid2symbol ++ ensemblid2symbol ++ entrezid2symbol ++ symbol2symbol

    val l1000OrNot = rawSignature
      .map(gene => (gene, tt.get(gene)))
      .map{case (gene, optionTranslation) =>
        (gene, optionTranslation.isDefined, tt.getOrElse(gene,""))}

    l1000OrNot.map{case (query, inL1000, symbol) =>
                      Map("query" -> query, "inL1000" -> inL1000, "symbol" -> symbol)
    }

  }

  def checkSignature = this.result _

}