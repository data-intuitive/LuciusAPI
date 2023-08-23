package com.dataintuitive.luciusapi

import com.dataintuitive.luciuscore._
import model.v4_1._
import genes._
import api.v4_1._
import io.GenesIO._
import io.{ Version, DatedVersionedObject, State }

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, typedLit, concat, array, col, row_number}
import org.apache.spark.sql.expressions.Window

import org.apache.hadoop.fs.{FileSystem, Path}

object IO {

  def getGenesDB(sparkSession: SparkSession, geneAnnotationsFile: String, dataTypeDict: Map[String, String] = Map.empty): GenesDB = {
    import sparkSession.implicits._
    
    val genesRaw = sparkSession.read.parquet(geneAnnotationsFile)
    val genesMapped = genesRaw
      .drop("gene_description", "probe_set_id")
      .withColumn("index",row_number().over(Window.orderBy(lit(1))))
      .withColumnRenamed("gene_id", "id")
      .withColumn("entrezid", typedLit(Option.empty[Set[String]]))
      .withColumn("ensemblid", typedLit(Option.empty[Set[String]]))
      .withColumn("symbol", array($"gene_symbol"))
      .withColumn("name", typedLit(Option.empty[Set[String]]))
      .withColumn("geneFamily", typedLit(Option.empty[Set[String]]))
      .withColumn("dataType1", col("landmark").cast("integer"))
      .withColumn("dataType2", col("best_inferred").cast("integer"))
      .withColumn("dataType", concat(col("dataType1"), lit("-"), col("dataType2")))
      .as[Gene]
      .collect
    
    val genesMappedDatatype = genesMapped.map(gene => gene.copy(dataType = dataTypeDict.get(gene.dataType).getOrElse(gene.dataType)))

    genes.GenesDB(genesMappedDatatype)
  }

  def allInput(sparkSession: SparkSession, path: List[String]):List[DatedVersionedObject[Path]] = {
    import sparkSession.implicits._

    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)

    val outputList =
      path.flatMap(p => {
        val pp = new Path(p)
        if (pp.toString.contains(".parquet"))
          List(pp)
            .map(x => (x.getName, x.getParent, x))
        else
          fs
            .listStatus(pp)
            .map(_.getPath)
            .map(x => (x.getName, x.getParent, x))
            .filter(_._1.toString() contains ".parquet")
      })
    val outputs = outputList.map{ case(name, path, fullPath) =>
      val p = sparkSession.read.parquet(fullPath.toString).as[Perturbation]
      val version:Version =
        p.first
          .meta
          .filter{ case MetaInformation(key, value) => key == "version"}
          .headOption
          .map(_.value)
          .map(Version(_))
          .getOrElse(Version(0,0))
      val dateStrO =
        p.first
          .meta
          .filter{ case MetaInformation(key, value) => key == "processingDate"}
          .headOption
          .map(_.value)
      val date = dateStrO.map(java.time.LocalDate.parse).getOrElse(java.time.LocalDate.MIN)
      DatedVersionedObject(date, version, fullPath)
    }.toList

    outputs

  }


}
