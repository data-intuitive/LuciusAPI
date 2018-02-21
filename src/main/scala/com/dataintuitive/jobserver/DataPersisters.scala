package com.dataintuitive.jobserver

import spark.jobserver._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._


/**
  * implementation of a NamedObjectPersister for DataSet objects
  *
  */
class DataSetPersister[T] extends NamedObjectPersister[NamedDataSet[T]] {

  override def persist(namedObj: NamedDataSet[T], name: String) {
    namedObj match {
      case NamedDataSet(ds, forceComputation, storageLevel) =>
        require(!forceComputation || storageLevel != StorageLevel.NONE,
                "forceComputation implies storageLevel != NONE")
        //these are not supported by DataFrame:
        //df.setName(name)
        //df.getStorageLevel match
        ds.persist(storageLevel)
        // perform some action to force computation
        if (forceComputation) ds.count()
    }
  }

  override def unpersist(namedObj: NamedDataSet[T]) {
    namedObj match {
      case NamedDataSet(ds, _, _) =>
        ds.unpersist(blocking = false)
    }
  }

  /**
    * Calls df.persist(), which updates the DataFrame's cached timestamp, meaning it won't get
    * garbage collected by Spark for some time.
    * @param namedDF the NamedDataFrame to refresh
    */
  override def refresh(namedDS: NamedDataSet[T]): NamedDataSet[T] =
    namedDS match {
      case NamedDataSet(ds, _, storageLevel) =>
        ds.persist(storageLevel)
        namedDS
    }

}
