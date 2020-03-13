package com.dataintuitive.luciusapi.functions

import com.dataintuitive.luciuscore.genes._
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

  case class JobData(db: Dataset[DbRow], genesDB: GenesDB, version: String, signature: List[String])

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

    val JobData(db, genesDB, version, rawSignature) = data
    implicit val genes = genesDB

    val tt = genesDB
              .createSymbolDictionary
              .map(_.swap)
              .flatMap{ case (gene, symbol) =>
                List(
                  (gene.probesetid, symbol),
                  (gene.ensemblid.map(_.toList.head).getOrElse("NA"), symbol),
                  (gene.symbol.map(_.toList.head).getOrElse("NA"), symbol))
              }

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
