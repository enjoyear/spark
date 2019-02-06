
// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

object MyStrategyImpl {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("MyStrategyImpl")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext

    // Set the broadcast join threshold to be 1MB
    spark.sql("SET spark.sql.autoBroadcastJoinThreshold=1048576")
    spark.sql("SET spark.sql.autoBroadcastJoinThreshold").show

    val tableA = spark.range(2000000).as('a)
    val tableB = spark.range(1000000).as('b)

    val result = tableA.join(tableB, tableA("id") === tableB("id")).groupBy().count()

    result.show()
    result.explain

    spark.stop()
  }
}

// scalastyle:on println
