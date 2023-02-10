package com.dataintuitive.luciusapi

// LuciusCore
import com.dataintuitive.luciuscore._
import model.v4_1._
import genes._
import api.v4_1._

// Jobserver
import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}
import spark.jobserver._

// Scala, Scalactic and Typesafe
import scala.util.Try
import org.scalactic._
import Accumulation._
import com.typesafe.config.Config
import collection.JavaConverters._

// Spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.dataintuitive.jobserver.NamedDataSet
import com.dataintuitive.jobserver.DataSetPersister

/**
  * Common functionality, encapsulating the fact that we want to run tests outside of jobserver as well.
  */
object Common extends Serializable {

  val VERSIONS = Set("v1", "v2", "t1")

  implicit def rddPersister[T]: NamedObjectPersister[NamedRDD[T]] = new RDDPersister[T]
  implicit def broadcastPersister[U]: NamedObjectPersister[NamedBroadcast[U]] =
    new BroadcastPersister[U]
  implicit def DataSetPersister[T]: NamedObjectPersister[NamedDataSet[T]] = new DataSetPersister[T]

  object ParamHandlers {

    def paramSignature(config: Config): List[String] Or One[ValidationProblem] = {
      Try(config.getString("query").split(" ").toList)
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Signature query not provided"))))
    }

    def paramSignature1(config: Config): List[String] Or One[ValidationProblem] = {
      Try(config.getString("query1").split(" ").toList)
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Signature query not provided"))))
    }

    def paramSignature2(config: Config): List[String] Or One[ValidationProblem] = {
      Try(config.getString("query2").split(" ").toList)
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Signature query not provided"))))
    }

    def paramCompoundQ(config: Config): String Or One[ValidationProblem] = {
      Try(config.getString("query"))
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Parameter compound query not provided"))))
    }

    def paramTargetQ(config: Config): String Or One[ValidationProblem] = {
      Try(config.getString("query"))
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Parameter target query not provided"))))
    }

    def paramTargets(config: Config): List[String] Or One[ValidationProblem] = {
      Try(config.getString("query").split(" ").toList)
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Parameter targets query not provided"))))
    }

    def paramCompounds(config: Config): List[String] Or One[ValidationProblem] = {
      Try(config.getString("query").split(" ").toList)
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Parameter compounds not provided"))))
    }

    def paramPerturbation(config: Config): String Or One[ValidationProblem] = {
      Try(config.getString("query"))
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Parameter perturbation query not provided"))))
    }

    def paramSamples(config: Config): List[String] Or One[ValidationProblem] = {
      Try(config.getString("samples").split(" ").toList)
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Parameter samples not provided"))))
    }

    def paramSorted(config: Config): Boolean Or One[ValidationProblem] = {
      Try(config.getString("sorted").toBoolean)
        .map(q => Good(q))
        .getOrElse(Bad(One(SingleProblem("Parameter sorted not provided or not boolean"))))
    }

    def optPValue(config: Config, default: Double = 0.05): Double = {
      Try(config.getString("pvalue").toDouble).getOrElse(default)
    }

    def optParamBinsX(config: Config, default: Int = 20): Int = {
      Try(config.getString("binsX").toInt).getOrElse(default)
    }

    def optParamHead(config: Config, default: Int = 0): Int = {
      Try(config.getString("head").toInt).getOrElse(default)
    }

    def optParamTail(config: Config, default: Int = 0): Int = {
      Try(config.getString("tail").toInt).getOrElse(default)
    }

    def validHeadTail(config: Config): Boolean Or One[ValidationProblem] = {
      // we only want either head or tail but not both, so 'exclusive or' needed instead of 'or', so use '!=" instead of '||'
      // (false, false) => false
      // (true, false)  => true
      // (false, true)  => true
      // (true, true)   => false
      if (optParamHead(config) > 0 != optParamTail(config) > 0) Good(true)
      else Bad(One(SingleProblem("Either head or tail count needs to be provided")))
    }

    def optParamBins(config: Config, default: Int = 15): Int = {
      Try(config.getString("binsX").toInt).getOrElse(default)
    }

    def optParamBinsY(config: Config, default: Int = 20): Int = {
      Try(config.getString("binsY").toInt).getOrElse(default)
    }

    def optParamSignature(config: Config, default: List[String] = List(".*")): List[String] = {
      Try(config.getString("query").split(" ").toList).getOrElse(default)
    }

    def optParamVersion(config: Config, default: String = "v2"): String = {
      Try(config.getString("version")).getOrElse(default)
    }

    /**
     * Parse a filter object with optional filters
     *
     * If the filter key is not present, return Map()
     *
     * If the filter key is present, we expect a form:
     *   filterKey: [ ... ]
     *
     * If filterKey exists, but the value can not be cast to `List[String]`, and the key is filtered out.
     */
    def optParamFilters(config: Config): Seq[(String, Seq[String])] =
        Try{
            val keys = config.getObject("filter")
                .unwrapped.asScala.toMap.map(_._1)

            val lists = keys.map(key => s"filter.$key").flatMap(x => Try(config.getStringList(x).asScala.toList).toOption)

            (keys zip lists).toSeq
        }.toOption
        .getOrElse(Seq())

    def validVersion(config: Config): String Or One[ValidationProblem] = {
      if (VERSIONS contains optParamVersion(config)) Good(optParamVersion(config))
      else Bad(One(SingleProblem("Not a valid version identifier")))
    }

    def optParamPwids(config: Config, default: List[String] = List(".*")): List[String] = {
      Try(config.getString("pwids").split(" ").toList).getOrElse(default)
    }

    def optParamLimit(config: Config, default: Int = 10): Int = {
      Try(config.getString("limit").toInt).getOrElse(default)
    }

    def optParamLike(config: Config, default: List[String] = Nil): List[String] = {
      Try(config.getString("like").split(" ").toList).getOrElse(default)
    }

    def optParamTrtType(config: Config, default: List[String] = Nil): List[String] = {
      Try(config.getString("trtType").split(" ").toList).getOrElse(default)
    }

    def optParamFeatures(config: Config, default: List[String] = List(".*")): List[String] = {
      Try(config.getString("features").toString.split(" ").toList).getOrElse(default)
    }

    def getDB(runtime: JobEnvironment): Dataset[Perturbation] Or One[ValidationProblem] = {
      Try {
        val NamedDataSet(db, _, _) = runtime.namedObjects.get[NamedDataSet[Perturbation]]("db").get
        db
      }.map(db => Good(db))
        .getOrElse(Bad(One(SingleProblem("Cached DB not available"))))
    }

    def getFlatDB(runtime: JobEnvironment): Dataset[FlatDbRow] Or One[ValidationProblem] = {
      Try {
        val NamedDataSet(flatdb, _, _) = runtime.namedObjects.get[NamedDataSet[FlatDbRow]]("flatdb").get
        flatdb
      }.map(flatdb => Good(flatdb))
        .getOrElse(Bad(One(SingleProblem("Cached FlatDB not available"))))
    }

    def getGenes(runtime: JobEnvironment): GenesDB Or One[ValidationProblem] = {
      Try {
        val NamedBroadcast(genes) = runtime.namedObjects.get[NamedBroadcast[GenesDB]]("genes").get
        genes.value
      }.map(genes => Good(genes))
        .getOrElse(Bad(One(SingleProblem("Broadcast genes not available"))))
    }

    def getFilters(runtime: JobEnvironment): Filters.FiltersDB Or One[ValidationProblem] = {
      Try {
        val NamedBroadcast(filters) = runtime.namedObjects.get[NamedBroadcast[Filters.FiltersDB]]("filters").get
        filters.value
      }.map(filters => Good(filters))
        .getOrElse(Bad(One(SingleProblem("Broadcast filters not available"))))
    }

    def paramDb(config: Config): String Or One[ValidationProblem] = {
      Try(config.getString("db.uri"))
        .map(db => Good(db))
        .getOrElse(Bad(One(SingleProblem("DB config parameter not provided"))))
    }

    def paramDbs(config: Config): List[String] Or One[ValidationProblem] = {
      Try(config.getStringList("db.uris").asScala.toList)
        .map(dbs => Good(dbs))
        .getOrElse(Bad(One(SingleProblem("DB config parameter not provided"))))
    }

    /**
     * Checks config for either db.uri or db.uris.
     * This allows for both using the older format db.uri as single string
     * or the newer format db.uris which allows a list of strings.
     * By supporting the old format we prevent old config files from breaking.
     */
    def paramDbOrDbs(config: Config): List[String] Or One[ValidationProblem] = {
      val singleDb = paramDb(config)
      val multipleDbs = paramDbs(config)

      (singleDb.isGood, multipleDbs.isGood) match {
        case (false, false) => Bad(One(SingleProblem("DB config parameter not provided")))
        case (true, true) => Bad(One(SingleProblem("Only one declaration of db.uri or db.uris is allowed")))
        case (true, false) => singleDb.map(List(_))
        case (false, true) => multipleDbs
      }
    }

    def paramGenes(config: Config): String Or One[ValidationProblem] = {
      Try(config.getString("geneAnnotations"))
        .map(ga => Good(ga))
        .getOrElse(Bad(One(SingleProblem("geneAnnotations config parameter not provided"))))
    }

    def paramPartitions(config: Config): Int = {
      // Optional parameter, default is 24
      Try(config.getString("partitions").toInt)
        .getOrElse(24)
    }

    def paramStorageLevel(config: Config): StorageLevel = {
      // Optional parameter, default is 24
      Try(config.getString("storageLevel")).toOption
        .flatMap(_ match {
          case "MEMORY_ONLY"   => Some(StorageLevel.MEMORY_ONLY)
          case "MEMORY_ONLY_2" => Some(StorageLevel.MEMORY_ONLY_2)
          case _               => None
        })
        .getOrElse(StorageLevel.MEMORY_ONLY)
    }

    def paramDbVersion(config: Config, default: String = "latest"): String = {
      Try(config.getString("db.version")).getOrElse(default)
    }

    val defaultDict = Map(
        "probesetID" -> "probesetid",
        "dataType" -> "dataType",
        "ENTREZID" -> "entrezid",
        "ENSEMBL" -> "ensemblid",
        "SYMBOL" -> "symbol",
        "GENENAME" -> "name",
        "GENEFAMILY" -> "geneFamily"
        )

    /**
     * geneFeatures contains the mapping between the input gene annotations file
     * and the features in the model.
     * This function parses this part of the config and falls back to the above if
     * `geneFeatures` is not present in the config.
     */
    def paramGeneFeatures(config: Config):Map[String, String] = {
      Try(config.getObject("geneFeatures")).toOption
          .map(_.unwrapped.asScala.toMap.map{case (k,v) => (k.toString, v.toString)})
          .getOrElse(defaultDict)
    }

    /**
     * geneDataType contains a mapping between the read dataType and how it should be
     * returned from the code.
     * This is especially useful when two dataTypes are read in together to be concatenated,
     */
    def paramGeneDataTypes(config: Config):Map[String, String] = {
      Try(config.getObject("geneDataType")).toOption
        .map(_.unwrapped.asScala.toMap.map{case (k,v) => (k.toString, v.toString)})
        .getOrElse(Map.empty)
    }

    /**
     * In order to make sure Lucius handles heavy load and slow response times,
     * it's important to dev/test with larger datasets than a small dev dataset allows.
     * This paramter multiplies input data this many times so the apparant dataset is larger.
     */
    def paramMultiplicity(config: Config):Int = {
      Try(config.getString("multiplicity").toInt)
        .getOrElse(1)
    }

  }

}
