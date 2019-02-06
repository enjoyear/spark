
// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Range}
import org.apache.spark.sql.execution.{ProjectExec, RangeExec, SparkPlan}
import org.apache.spark.sql.{SparkSession, Strategy}

object MyStrategyImpl {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("MyStrategyImpl")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext

    spark.sqlContext.experimental.extraStrategies = IntervalJoin :: Nil

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

case object IntervalJoin extends Strategy with Serializable {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Join(
    Range(start1, end1, 1, part1, Seq(o1), isStreaming1),
    Range(start2, end2, 1, part2, Seq(o2), isStreaming2),
    Inner,
    Some(EqualTo(e1, e2)))
      if ((o1 semanticEquals e1) && (o2 semanticEquals e2)) ||
        ((o1 semanticEquals e2) && (o2 semanticEquals e1)) => {
      if ((start1 <= end2) && end1 >= end2) {
        val start = math.max(start1, start2)
        val end = math.min(end1, end2)
        val part = math.max(part1.getOrElse(200), part2.getOrElse(200))
        val result = RangeExec(Range(start, end, 1, Some(part), o1 :: Nil, false))
        val twoColumns = ProjectExec(Alias(o1, o1.name)(exprId = o1.exprId) :: Nil, result)
        twoColumns :: Nil
      }
      else {
        Nil
      }
    }
    case _ => Nil
  }
}

// scalastyle:on println
