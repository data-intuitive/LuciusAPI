package com.dataintuitive.luciusapi.deprecated

import com.dataintuitive.luciuscore.Model._
import com.dataintuitive.luciuscore.SignatureModel.SymbolSignature
import com.dataintuitive.luciuscore.TransformationFunctions._
import com.dataintuitive.luciuscore.ZhangScoreFunctions._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import spark.jobserver._

import scalaz.Scalaz._

import scala.util.Try

/**
  * Endpoint provided for backward compatibility, superseded by `histogram`
  */
object targetHistogram extends SparkJob with NamedRddSupport with Globals {

  import OldCommon._

  val helpMsg =
    s"""
     """.stripMargin

  val simpleChecks:SingleParValidations = Seq()

  val combinedChecks:CombinedParValidations = Seq()

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {

    val showHelp = Try(config.getString("help")).toOption.isDefined

    showHelp match {
      case true => SparkJobInvalid(helpMsg)
      case false => SparkJobValid
    }

  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    // API Version
    val version = Try(config.getString("version")).getOrElse("v1")
    // sample query string
    val signatureQuery:String = Try(config.getString("query")).getOrElse("*")
    val signatureSpecified = !(signatureQuery == "*")
    val rawSignature = signatureQuery.split(" ")
    // features to return: list of features
    val featuresString:String = Try(config.getString("features")).getOrElse("zhang")
    val featuresQuery = featuresString.split(" ").toList
    // The number of bins
    val binsString:String = Try(config.getString("bins")).getOrElse("15")


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

    // Histogram functionality
    val nrBuckets = binsString.toInt
    val delta = 200/(nrBuckets-1)
    val rounding = 100

    val lowerBounds = (-rounding until rounding by delta)
      .map(_.toDouble / rounding)
    val upperBounds = lowerBounds.drop(1) :+ 1.0
    val rangesTmp = lowerBounds zip upperBounds zipWithIndex
    val ranges = rangesTmp.map{case((l, u), i) => (i,l, u)}

    // calculate zhang scores and add platewellid and index after sorting
    // This is time-consuming, but we do not want to maintain state in the backend
//    val annotatedSimilarity = aRanks.map(x => (connectionScore(x.ranks, query),x))
//                                  .sortBy{case (z, x) => -z}
//                                  .zipWithIndex
//                                  .map{case ((z, x),index) => (index, z, x)}

    // Add Zhang score if signature is present
    val zhangAdded =
        db
          .flatMap{updateZhang(_, query)}
          .sortBy{case (z, x) => -z}

    val zhangAddedwithIndex =
        zhangAdded
          .zipWithIndex
          .map{case ((z, x),index) => (index, z, x)}


    // Generate the frequency counts for the bins defined in `ranges`.
    // All available targets are covered and the similarity/zhang score is added as a feature as well.
    // Scalaz semigroup is used for the _summing_ of the _Maps_
    // reduceByKey is used for performance over groupByKey.
    // Zhang frequency is added as a feature
    // TODO:
    //   - Extract 2 functions and add them to luciusback: target extraction and range checking
    // EXTENSION:
    //   - also track bounds of intervals
    val binned = zhangAddedwithIndex
      .map{ case(index, zhang, row) => (index, zhang, row.pwid, row.compoundAnnotations.knownTargets.toList.map(t => (t,1)).toMap) }
      .map{ case(index, zhang, pwid, targetsMap) =>
                    (
                      ranges.filter{case (i, l, u) => (l <= zhang && zhang <= u)}.map(_._1).head,     // key   = bin
                      (index, index, Map("zhang" -> 1) ++ targetsMap)                        // value = triple
                    )
                    }
      .reduceByKey{(left, right) => (Math.min(left._1, right._1), Math.max(left._2, right._2), left._3 |+| right._3)}

    // extract the features we are interested in from the RDD and collect
    val histogram = binned
      .map{case (bin, (l, u, m)) => (bin,
                                      Map("bounds" -> Array(l,u)) ++
                                      featuresQuery.map( feature => (feature, m.getOrElse(feature,0))).toMap)
      }
      .collectAsMap

    val nrFeatures = featuresQuery.size

    // Useful later in getOrElse
    val emptyMap = Map("bounds" -> Array()) ++ featuresQuery.map(feature => (feature,0)).toMap

    // Join ranges with aggregated data
    val res = ranges.reverse.map{case(bin, l, u) => histogram.getOrElse(bin, emptyMap)}

    // Create two datastructures for output:
    val metadata = Map( "bins"   -> res.size,
                        "bounds" -> res.map(_.getOrElse("bounds", Array())))
    val data = featuresQuery.flatMap(feature => Map(feature -> res.map(_.getOrElse(feature, 0)))).toMap

    Map("metadata" -> metadata) ++
    Map("data" -> data)

  }

}

