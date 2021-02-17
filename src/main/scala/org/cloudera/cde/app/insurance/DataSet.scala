package org.cloudera.cde.app.insurance

import org.apache.spark.sql.{DataFrame, Dataset}

trait DataSet {

  def extract(): DataFrame

  def transform(dataSet:DataFrame): DataFrame

  def load(dataSet:DataFrame): Unit

  def etl(): Unit = {
    load(transform(extract()))

  }
}
