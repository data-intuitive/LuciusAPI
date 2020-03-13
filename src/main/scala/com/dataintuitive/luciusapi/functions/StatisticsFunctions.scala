package com.dataintuitive.luciusapi.functions

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.dataintuitive.luciuscore.genes._
import com.dataintuitive.luciuscore.Model.DbRow
import scala.collection.immutable.Map
import com.dataintuitive.luciusapi.Model.FlatDbRow

object StatisticsFunctions extends SessionFunctions {

  case class JobData(db: Dataset[DbRow], flatDb: Dataset[FlatDbRow], genes: GenesDB)
  type JobOutput = Map[String, Any]

  val helpMsg =
    "Return general statistics about the dataset.\nNo input is required. Pass null for parameters in Scala"

  def info(data: JobData) = "General statistics about the dataset"

  def header(data: JobData) = Map("key" -> "value").toString

  def result(data: JobData)(implicit sparkSession: SparkSession) = {

    import sparkSession.implicits._

    val flatDb = data.flatDb

    val compounds = Map(
      "total" -> flatDb
        .filter($"protocolname" !== "")
        .select($"jnjs")
        .distinct
        .count,
      "mcf7" -> flatDb
        .filter($"protocolname" === "MCF7")
        .select($"jnjs")
        .distinct
        .count,
       "pbmc" -> flatDb
        .filter($"protocolname" === "PBMC")
        .select($"jnjs")
        .distinct
        .count
     )

    val samples = Map(
      "total" -> flatDb
        .filter($"protocolname" !== "")
        .select($"pwid")
        .distinct
        .count,
      "mcf7" -> flatDb
        .filter($"protocolname" === "MCF7")
        .select($"pwid")
        .distinct
        .count,
      "pbmc" -> flatDb
        .filter($"protocolname" === "PBMC")
        .select($"pwid")
        .distinct
        .count
    )

    val informative = Map(
      "total" -> flatDb
        .filter($"protocolname" !== "")
        .filter($"informative")
        .count,
      "mcf7" -> flatDb
        .filter($"protocolname" === "MCF7")
        .filter($"informative")
        .count,
      "pbmc" -> flatDb
        .filter($"protocolname"=== "PBMC")
        .filter($"informative")
        .count
    )

    Map("samples" -> samples, "compounds" -> compounds, "informative" -> informative)
  }

//   def statistics = result _

}
