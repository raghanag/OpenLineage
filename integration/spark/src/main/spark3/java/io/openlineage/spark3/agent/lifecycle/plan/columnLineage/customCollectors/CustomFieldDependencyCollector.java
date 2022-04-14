package io.openlineage.spark3.agent.lifecycle.plan.columnLineage.customCollectors;

import io.openlineage.spark3.agent.lifecycle.plan.columnLineage.ColumnLevelLineageBuilder;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public interface CustomFieldDependencyCollector {

  boolean isDefinedAt(LogicalPlan plan);

  void collect(LogicalPlan plan, ColumnLevelLineageBuilder builder);
}
