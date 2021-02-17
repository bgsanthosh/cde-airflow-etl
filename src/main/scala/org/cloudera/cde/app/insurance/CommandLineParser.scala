package org.cloudera.cde.app.insurance

import com.github.acrisci.commander.Program

case class AppOptions(dataSetBucketPath: String, schema: String, writeBucketPath: String)

object CommandLineParser {

  def parse(args: Array[String]): AppOptions = {
    val program: Program = new Program()
      .version("0.0.1")
      .option(flags="-d, --dataSetBucketPath [type]", description="data set bucket path", default="s3a://XXXXXXXXXXXXXXXXXXXXXXXXv/test-data/insurance_data", required=false)
      .option(flags="-s, --schema [type]", description="schema to be created", default="airflow_cde_etl", required=false)
      .option(flags="-w, --writeBucketPath [type]", description="write to s3 bucket", default="s3a://XXXXXXXXXXXXXXXXXXXed-env/test_result/airflow", required=false)
    program.parse(args)
    AppOptions(program.dataSetBucketPath, program.schema, program.writeBucketPath)
  }
}

