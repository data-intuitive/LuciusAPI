package com.dataintuitive.luciusapi

// Functions implementation and common code
import Common.ParamHandlers._

// LuciusCore
import com.dataintuitive.luciuscore.Model.DbRow
import com.dataintuitive.luciuscore.genes._
import com.dataintuitive.luciuscore.TransformationFunctions
import com.dataintuitive.luciuscore.signatures._

// Jobserver
import spark.jobserver.api.{JobEnvironment, SingleProblem, ValidationProblem}
import spark.jobserver._

// Scala, Scalactic and Typesafe
import scala.util.Try
import org.scalactic._
import Accumulation._
import com.typesafe.config.Config

// Spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object generateSignature extends SparkSessionJob with NamedObjectSupport {

  case class JobData(db: Dataset[DbRow], genesDB: GenesDB, pValue:Double, version: String, samples: List[String])

  type JobOutput = Array[String]

  override def validate(sparkSession: SparkSession,
                        runtime: JobEnvironment,
                        config: Config): JobData Or Every[ValidationProblem] = {

    val db = getDB(runtime)
    val genes = getGenes(runtime)
    val pValue = optPValue(config)
    val version = optParamVersion(config)
    val isValidVersion = validVersion(config)
    val samples = paramSamples(config)

    (isValidVersion zip
      withGood(db, genes, samples) {
        JobData(_, _, pValue, version, _)
      }).map(_._2)

  }

  override def runJob(sparkSession: SparkSession,
                      runtime: JobEnvironment,
                      data: JobData): JobOutput = {

    implicit val thisSession = sparkSession

    val JobData(db, genesDB, pValue, version, samples) = data

    implicit val genes = genesDB

    // Start with query compound and retrieve indices
    val selection =
      db.filter(x => x.id.exists(elem => samples.toSet.contains(elem)))
        .collect
        .map(x => (x.sampleAnnotations.t.get, x.sampleAnnotations.p.get))
    val valueVector = TransformationFunctions.aggregateStats(selection, pValue)
    val rankVector = TransformationFunctions.stats2RankVector((valueVector, Array()))

    val indexSignature: IndexSignature =
      TransformationFunctions.rankVector2IndexSignature(rankVector)

    val symbolSignature = indexSignature.toSymbolSignature

    symbolSignature.toArray

  }

}
