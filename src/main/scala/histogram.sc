package luciusapi

import LuciusBack.AnnotatedRanks
import LuciusBack.GeneFunctions._
import LuciusBack.L1000._
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.jobserver._
import scalaz._
import scalaz.Scalaz._
import scala.util.Try

object targetHistogram extends SparkJob with NamedRddSupport {

  val _namedRdds = initialize.namedRdds

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    val namespace = sc.getConf.get("spark.app.name")

    val rdd = _namedRdds.get[String](namespace + "genes")
    if (rdd.isDefined) SparkJobValid else SparkJobInvalid(s"Missing named RDD [genes]")

  }

  override def runJob(sc: SparkContext, config: Config): Any = {

    val namespace = sc.getConf.get("spark.app.name")

    // Load cached RDDs, depending on the mode we're in
    val jobServerRunning = Try(_namedRdds).toOption
    val aRanks = if (jobServerRunning.isDefined) {
      _namedRdds.get[AnnotatedRanks](namespace + "aRanks").get
    } else {
      sc.getPersistentRDDs.map{case (index, rdd) => rdd}.filter(rdd => (rdd.name == "aRanks")).head.asInstanceOf[RDD[AnnotatedRanks]]
    }
    val genes = if (jobServerRunning.isDefined) {
      _namedRdds.get[String](namespace + "genes").get.collect
    } else {
      sc.getPersistentRDDs.map { case (index, rdd) => rdd }.filter(rdd => (rdd.name == "genes")).head.asInstanceOf[RDD[String]].collect
    }
    val translationTable = if (jobServerRunning.isDefined) {
      _namedRdds.get[(String, String)](namespace + "translationTable").get.collect
    } else {
      sc.getPersistentRDDs.map { case (index, rdd) => rdd }.filter(rdd => (rdd.name == "translationTable")).head.asInstanceOf[RDD[(String,String)]].collect
    }

    val featuresString:String = Try(config.getString("features")).getOrElse("zhang")
    val binsString:String = Try(config.getString("bins")).getOrElse("15")

    // signature
    val queryString:String = Try(config.getString("query")).getOrElse("*")
    val untranslatedOrderedSignature = queryString.split(" ").toArray
    val translatedOrderedSignature = translateGenes(untranslatedOrderedSignature, translationTable.toMap)
    val query = orderedSignature2RankVector(translatedOrderedSignature, genes)

    val features = featuresString.split(" ").toList

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
    val annotatedSimilarity = aRanks.map(x => (connectionScore(x.ranks, query),x))
                                  .sortBy{case (z, x) => -z}
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
    val binned = annotatedSimilarity
      .map{ case(i,z,x) => (i, z, x.idx, x.id, x.targets.split(",").map(_.trim).map(t => (t,1)).toMap) }
      .map{ case(i, z, idx, id, m) =>
      (ranges.filter{case (i, l, u) => (l <= z && z <= u)}.map(_._1).head,     // key   = bin
        (i, i, Map("zhang" -> 1) ++ m)                                          // value = triple
        )
    }
      .reduceByKey{(left, right) => (Math.min(left._1, right._1), Math.max(left._2, right._2), left._3 |+| right._3)}

    // extract the features we are interested in from the RDD and collect
    val histogram = binned
      .map{case (bin, (l, u, m)) => (bin,
                                      Map("bounds" -> Array(l,u)) ++
                                      features.map( feature => (feature, m.getOrElse(feature,0))).toMap)
      }
      .collectAsMap

    val nrFeatures = features.size

    // Useful later in getOrElse
    val emptyMap = Map("bounds" -> Array()) ++ features.map(feature => (feature,0)).toMap

    // Join ranges with aggregated data
    val res = ranges.reverse.map{case(bin, l, u) => histogram.getOrElse(bin, emptyMap)}

    // Create two datastructures for output:
    val metadata = Map( "bins"   -> res.size,
                        "bounds" -> res.map(_.getOrElse("bounds", Array())))
    val data = features.flatMap(feature => Map(feature -> res.map(_.getOrElse(feature, 0)))).toMap

    Map("metadata" -> metadata) ++
    Map("data" -> data)


  }

}

