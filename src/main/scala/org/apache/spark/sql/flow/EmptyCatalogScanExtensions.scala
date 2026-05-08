package org.apache.spark.sql.flow

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{LocalTableScanExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}

class EmptyCatalogScanExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(_ => EmptyCatalogScanStrategy)
  }
}

private object EmptyCatalogScanStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case relation: HiveTableRelation =>
      emptyScan(relation)

    case relation: LogicalRelation if relation.catalogTable.isDefined =>
      emptyScan(relation)

    case relation: DataSourceV2Relation =>
      emptyScan(relation)

    case relation: DataSourceV2ScanRelation =>
      emptyScan(relation)

    case _ =>
      Nil
  }

  private def emptyScan(plan: LogicalPlan): Seq[SparkPlan] = {
    LocalTableScanExec(plan.output, Seq.empty[InternalRow]) :: Nil
  }
}
