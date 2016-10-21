package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.SignatureModel.SymbolSignature
import com.typesafe.config.Config
import org.apache.spark._
import com.dataintuitive.luciuscore.TransformationFunctions._
import com.dataintuitive.luciuscore.ZhangScoreFunctions._

import scala.util.Try
import spark.jobserver.{NamedRddSupport, SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

/**
  * Returns the Zhang/similarity scores for the whole database for a given query.
  *
  * Input:
  *
  * - __`query`__: The gene signature (any notation will do) as a space separated list
  *
  * - __`sorted`__: Should the result be sorted on by Zhang score (descending)
  *
  * - __`version`__: Version of the API. "v2" returns `info` and `header`.
  *
  * Remark: Make sure you check the signature first, through checkSignature
  */
object zhang extends SparkJob with NamedRddSupport with Globals {

  import Common._

  type Output = Map[String, Any]
  type OutputData = Seq[(Long, Double, Long, String)]

  val helpMsg =
    s"""
     """.stripMargin

  val simpleChecks:SingleParValidations = Seq(
    ("query",   (isDefined ,    "query not defined in POST config")),
    ("query",   (isNotEmpty ,   "query is empty in POST config"))
  )

  val combinedChecks:CombinedParValidations = Seq()

// TODO: sorted = boolean !
  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {

    val showHelp = Try(config.getString("help")).toOption.isDefined
    val testsSingle = runSingleParValidations(simpleChecks, config)
    val testsCombined = runCombinedParValidations(combinedChecks, config)
    val allTests = aggregateValidations(testsSingle ++ testsCombined)

    (showHelp, allTests._1) match {
      case (true, _) => SparkJobInvalid(helpMsg)
      case (false, true) => SparkJobValid
      case (false, false) => SparkJobInvalid(allTests._2)
    }

  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    // API Version
    val version = Try(config.getString("version")).getOrElse("v1")
    // Compound query string
    val signatureQuery:String = Try(config.getString("query")).getOrElse("")
    val rawSignature = signatureQuery.split(" ")
    // Sort the similarities?
    val sortResult:Boolean = Try(config.getString("sorted").toBoolean).getOrElse(true)

    // Load cached data
    val db = retrieveDb(sc, this)
    val genes = retrieveGenes(sc, this).value

    // TODO: Make sure we continue with all symbols, or just make the job invalid when it isn't!
    val signature = SymbolSignature(rawSignature)

    val symbolDict = genes.symbol2ProbesetidDict
    val indexDict = genes.index2ProbesetidDict
    val iSignature = signature
                      .translate2Probesetid(symbolDict)
                      .translate2Index(indexDict)

    val vLength = db.first.sampleAnnotations.t.get.length
    val query = signature2OrderedRankVector(iSignature, vLength)

    // Calculate Zhang score for all entries that contain a rank vector
    // This should be used in a flatMap
    def updateZhang(x:DbRow, query:Array[Double]):Option[(Double, DbRow)] = {
      x.sampleAnnotations.r match {
        case Some(r) => Some((connectionScore(r, query), x))
        case _ => None
        }
    }

    val zhangAdded = db.flatMap{updateZhang(_, query)}

    // Index before (i) and after (j) sorting:
    val annotatedSimilarity =
      if (sortResult)
        zhangAdded
          .zipWithIndex
          .sortBy{case ((z,x), i) => -z}
          .zipWithIndex
          .map{case (((z,x), i), j) => (j, z, i, x.pwid.getOrElse(""))}
      else
        zhangAdded
          .zipWithIndex
          .map{case ((z,x), i) => (i, z, i, x.pwid.getOrElse(""))}

    val result:OutputData = annotatedSimilarity.collect

    val infoString =
      if (sortResult) "Zhang scores, sorted wrt descending Zhang score"
      else "Zhang scores, sorted wrt order in the database"

    version match {
      case "v2" => Map(
        "info"   -> infoString,
        "header" -> Seq("index after sorting", "zhang", "original index", "pwid"),
        "data"   -> result
      )
      case _ => result
    }

  }

}