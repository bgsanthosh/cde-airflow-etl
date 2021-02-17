package org.cloudera.cde.app

import org.cloudera.cde.app.insurance.{AppOptions, CommandLineParser, ProviderDataSet}
import SparkSetup._


object Application extends App {
  val options: AppOptions = CommandLineParser.parse(args)
  println("Started the Airflow App.")
  InsuranceApp.run(options)
}

object InsuranceApp {

  def run(options: AppOptions): Unit = {
    try {
      // create the database if not exists


      // etl provider dataset
      val provider = new ProviderDataSet(options)
      provider.etl()

    }
    finally {
      println("Completed the Airflow App.")
      spark.stop()
    }
  }

}
