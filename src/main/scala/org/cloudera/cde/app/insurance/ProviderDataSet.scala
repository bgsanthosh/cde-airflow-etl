package org.cloudera.cde.app.insurance

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode}
import org.cloudera.cde.app.SparkSetup.{spark, sqlContext}


object ProviderDataSet {

  val dataSetFileName = "Provider.csv"
}

case class ProviderData(provider: String,
                         potentialFraud: String)


class ProviderDataSet(val options: AppOptions) extends DataSet {

  override def transform(dataFrame: DataFrame): DataFrame = {
    dataFrame
  }

  override def load(dataFrame: DataFrame): Unit = {
    val schema: StructType = Encoders.product[ProviderData].schema
    val columns = for( f <- schema.fields) yield  f.name + " " + f.dataType.typeName
    val dColumns = columns.mkString(",")


    println("Writing to S3 bucket " + options.writeBucketPath + "/" + options.schema + "/provider")
    val df = sqlContext.sql("select * from providerDataFrame")
    df.select("Provider", "PotentialFraud").write.format("csv").mode(SaveMode.Overwrite).option("header", "true")
      .save(options.writeBucketPath + "/" + options.schema + "/provider" )
  }

  override def extract(): DataFrame = {

    val filePath = options.dataSetBucketPath + "/" + ProviderDataSet.dataSetFileName
    val schema: StructType = Encoders.product[ProviderData].schema
    val providerDataFrame = spark.sqlContext
      .read
      .schema(schema)
      .format("csv")
      .option("header", "true")
      .load(filePath)
    providerDataFrame.printSchema()
    providerDataFrame.createOrReplaceTempView("providerDataFrame")
    providerDataFrame
  }

}
