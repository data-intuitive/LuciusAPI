package com.dataintuitive.luciusapi.binning

import com.dataintuitive.luciuscore.Model.DbRow

import scalaz.Scalaz._
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map

/**
  * Created by toni on 29/11/16.
  */
object BinningFunctions {

  def histogram1D(sortedRdd:RDD[Double], nrBins:Int, lowerBound: Double, upperBound: Double):Array[Map[String, BigDecimal]] = {

    /**
      * Take Double's in, return BigDecimal for use internally
      */
    def createBins(nrBins:Int, lowerBound:BigDecimal, upperBound:BigDecimal):List[(Int, BigDecimal, BigDecimal)] = {

      val binSize = (upperBound - lowerBound) / nrBins
      val lowerBounds = (0 to nrBins-1).map{bin =>
        lowerBound + (bin * binSize)
      }
      val upperBounds = (1 to nrBins).map{bin =>
        lowerBound + (bin * binSize)
      }

      (nrBins -1 to 0 by -1)
        .zip(lowerBounds)
        .zip(upperBounds)
        .map{case ((i,j),k) => (i,j.setScale(4,BigDecimal.RoundingMode.HALF_UP),k.setScale(4,BigDecimal.RoundingMode.HALF_UP))}
        .sortBy(-_._1)
        .toList
    }

    /**
      * Please note that if x is outside the interval, the middle bin is selected
      */
    def inBin(x: Double, bins:List[(Int, BigDecimal, BigDecimal)]):Int = {
      val fallBack = -1
      bins.find{ case(bin, lower, upper) => (lower <= x) && (x <= upper)}.map(_._1).getOrElse(fallBack)
    }

    val bins:List[(Int, BigDecimal, BigDecimal)] = createBins(nrBins, lowerBound, upperBound)
    val binsAsMap = bins.map{case (bin, lowerBound, upperBound) =>
      (bin, Map("bin" -> BigDecimal(bin).round(new java.math.MathContext(2)),
        "lowerBound" -> lowerBound,
        "upperBound" -> upperBound,
        "count" -> BigDecimal(0)))
    }.toMap

    val nrIdx = sortedRdd.count

    // Sort by Zhang score, add an index and add the bin
    val idxBinAdded = sortedRdd
      .zipWithIndex
      .map{case (z,idx) => (inBin(z, bins), (z, idx))}

    // Calculate aggregates in one pass:
    val aggregates =
    idxBinAdded
      .map{case (bin, (z, idx)) => (bin, (z, 1, idx, idx, z, z))}
      .reduceByKey((x,y) =>
        (x._1 + y._1,             // sum Zhang scores
          x._2 + y._2,            // sum counts
          Math.min(x._3, y._3),   // min index
          Math.max(x._4, y._4),   // max index
          Math.min(x._5, y._5),   // min Zhang scores
          Math.max(x._6, y._6)))  // max Zhang scores
      .map{case (bin, (sumZhang, count, indexLow, indexHigh, zhangLow, zhangHigh)) =>
      (bin,  Map("zhangAvg" -> BigDecimal(sumZhang/count),
        "count" -> BigDecimal(count),
        "indexLow" -> BigDecimal(indexLow),
        "indexHigh" -> BigDecimal(indexHigh),
        "zhangLow" -> BigDecimal(zhangLow),
        "zhangHigh" -> BigDecimal(zhangHigh)))}
      .collectAsMap.toMap

    (binsAsMap |+| aggregates).values.toList.sortBy(_("bin")).toArray

  }

  def histogram2D(sortedRdd:RDD[(Double,List[String])], featuresInclZhang:List[String], nrBins:Int, lowerBound: Double, upperBound: Double) = {

    /**
      * Take Double's in, return BigDecimal for use internally
      */
    def createBins(nrBins:Int, lowerBound:BigDecimal, upperBound:BigDecimal):List[(Int, BigDecimal, BigDecimal)] = {

      val binSize = (upperBound - lowerBound) / nrBins
      val lowerBounds = (0 to nrBins-1).map{bin =>
        lowerBound + (bin * binSize)
      }
      val upperBounds = (1 to nrBins).map{bin =>
        lowerBound + (bin * binSize)
      }

      (nrBins -1 to 0 by -1)
        .zip(lowerBounds)
        .zip(upperBounds)
        .map{case ((i,j),k) => (i,j.setScale(4,BigDecimal.RoundingMode.HALF_UP),k.setScale(4,BigDecimal.RoundingMode.HALF_UP))}
        .sortBy(-_._1)
        .toList
    }

    /**
      * Please note that if x is outside the interval, the middle bin is selected
      */
    def inBin(x: Double, bins:List[(Int, BigDecimal, BigDecimal)]):Int = {
      val fallBack = -1
      bins.find{ case(bin, lower, upper) => (lower <= x) && (x <= upper)}.map(_._1).getOrElse(fallBack)
    }

    val bins:List[(Int, BigDecimal, BigDecimal)] = createBins(nrBins, lowerBound, upperBound)
    val binsAsMap = bins.map{case (bin, lowerBound, upperBound) =>
      (bin, Map("bin" -> BigDecimal(bin).round(new java.math.MathContext(2)),
        "lowerBound" -> lowerBound,
        "upperBound" -> upperBound,
        "count" -> BigDecimal(0)))
    }.toMap

    val nrIdx = sortedRdd.count

    // Sort by Zhang score, add an index and add the bin
    val idxBinAdded = sortedRdd
      .zipWithIndex
      .map{case ((z,targets), idx) => (inBin(z, bins), (z, targets, idx))}

    val features = featuresInclZhang.filter(_ != "zhang")

    // Calculate aggregates in one pass:
    val aggregates =
    idxBinAdded
      .map{case (bin, (z, targets, idx)) => (bin, (z, 1, idx, idx, z, z, (features.map(t => Map(t ->0)) ++ targets.map(t => Map(t -> 1))).reduce( _ |+| _) ))}
      .reduceByKey((x,y) =>
        (x._1 + y._1,             // sum Zhang scores
          x._2 + y._2,            // sum counts
          Math.min(x._3, y._3),   // min index
          Math.max(x._4, y._4),   // max index
          Math.min(x._5, y._5),   // min Zhang scores
          Math.max(x._6, y._6),  // max Zhang scores
          x._7 |+| y._7
          ))
      .map{case (bin, (sumZhang, count, indexLow, indexHigh, zhangLow, zhangHigh, targetMaps)) =>
        (bin,  Map("zhangAvg" -> BigDecimal(sumZhang/count),
          "count" -> BigDecimal(count),
          "indexLow" -> BigDecimal(indexLow),
          "indexHigh" -> BigDecimal(indexHigh),
          "zhangLow" -> BigDecimal(zhangLow),
          "zhangHigh" -> BigDecimal(zhangHigh))
          ++ targetMaps.mapValues(BigDecimal(_))
          )}
      .collectAsMap.toMap

    (binsAsMap |+| aggregates).values.toList.sortBy(_("bin")).toArray

  }

  def binZhang(x: Double, nrBins:Int) = {
    // x in [-1,+1]
    val xShift = x + 0.99999999
    // xShift in [0, 2]
    val rangeMax = 0.0
    val rangeMin = -2.0
    val binSize = (rangeMax - rangeMin) / nrBins
    Math.floor(xShift / binSize).toInt
  }

  def binIdx(x: Long, max:Long, nrBins:Int) = {
    // x in [0,\infinity]
    val rangeMax = max
    val rangeMin = 0
    val binSize = (rangeMax - rangeMin) / nrBins
    Math.floor(x / binSize).toInt
  }

  type Index = Long
  type Zhang = Double

  def bin2D(rdd:RDD[Zhang], nrBinsZhang:Int, nrBinsIdx:Int) = {

    val nrIdx = rdd.count

    // Sort by Zhang score, add an index and add the binned x-y coordinates
    val idxBinAdded = rdd
      .sortBy(-_)
      .zipWithIndex
      .map{case (z,idx) => ((binIdx(idx, nrIdx, nrBinsIdx),binZhang(z,nrBinsZhang)), (z, idx))}

    // Calculate aggregates in one pass:
    val aggregates =
    idxBinAdded
      .map{case ((x,y), ((z, idx))) => ((x,y), (z, 1))}
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .map{case ((x,y), ((sumZhang, count))) => ((x,y), Map("avgZhang" -> sumZhang/count, "count" -> count))}
      .collectAsMap

    // Prepare datastructure for plotting the data
    val bins:Seq[Map[String,Any]] = aggregates
      .toList
      .map{case (bin, m) =>
        Map("bin" -> (bin._1.toString + "-" + bin._2.toString),
          "x" -> bin._1,
          "y" -> bin._2,
          "count" -> m.getOrElse("count",0),
          "avg" -> m.getOrElse("avgZhang",0.0))}

    bins
  }

  def inBin2D(orig:(Index, Zhang), bin:(Int,Int), bins:(Int,Int), maxIdx:Long):Boolean = {
    val (nrBinsIdx, nrBinsZhang) = bins
    val (x,y) = bin
    val (idx, zhang) = orig

    (binIdx(idx, maxIdx, nrBinsIdx) == x) & (binZhang(zhang,nrBinsZhang) == y)

  }



  def bin1D(rdd:RDD[Zhang], nrBinsIdx:Int) = {

    val nrIdx = rdd.count

    // Sort by Zhang score, add an index and add the binned x-y coordinates
    val idxBinAdded = rdd
      .sortBy(-_)
      .zipWithIndex
      .map{case (z,idx) => (binIdx(idx, nrIdx, nrBinsIdx), (z, idx))}

    // Calculate aggregates in one pass:
    val aggregates =
    idxBinAdded
      .map{case (x, (z, idx)) => (x, (z, 1, idx, idx))}
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2, Math.min(x._3, y._3), Math.max(x._4, y._4)))
      .map{case (x, (sumZhang, count, indexLow, indexHigh)) => (x, Map("avgZhang" -> sumZhang/count, "count" -> count, "indexLow" -> indexLow, "indexHigh" -> indexHigh))}
      .collectAsMap

    // Prepare datastructure for plotting the data
    val bins:Seq[Map[String,Any]] = aggregates
      .toList
      .sortBy{case (bin, m) => bin}
      .map{case (bin, m) =>
        Map("bin" -> ("bin_" + bin.toString),
          "x" -> bin,
          "count" -> m.getOrElse("count",0),
          "avg" -> m.getOrElse("avgZhang",0.0),
          "indexLow" -> m.getOrElse("indexLow", 0),
          "indexHigh" -> m.getOrElse("indexHigh", 1000000L))
      }

    bins
  }


}
