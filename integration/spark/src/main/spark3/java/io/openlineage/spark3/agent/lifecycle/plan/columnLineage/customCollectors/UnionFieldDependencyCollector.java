package io.openlineage.spark3.agent.lifecycle.plan.columnLineage.customCollectors;

import static io.openlineage.spark3.agent.lifecycle.plan.columnLineage.FieldDependenciesCollector.traverseExpression;

import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark3.agent.lifecycle.plan.columnLineage.ColumnLevelLineageBuilder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Union;

@Slf4j
public class UnionFieldDependencyCollector implements CustomFieldDependencyCollector {

  public boolean isDefinedAt(LogicalPlan plan) {
    return plan instanceof Union;
  }

  public void collect(LogicalPlan plan, ColumnLevelLineageBuilder builder) {
    Union union = (Union) plan;

    // implement in Java code equivalent to Scala 'children.map(_.output).transpose.map { attrs =>'
    List<LogicalPlan> children = ScalaConversionUtils.<LogicalPlan>fromSeq(union.children());
    List<ArrayList<Attribute>> childrenAttributes = new LinkedList<>();
    children.stream()
        .map(child -> ScalaConversionUtils.<Attribute>fromSeq(child.output()))
        .forEach(list -> childrenAttributes.add(new ArrayList<>(list)));

    // max attributes size
    int maxAttributeSize =
        childrenAttributes.stream().map(list -> list.size()).max(Integer::compare).get();

    IntStream.range(0, maxAttributeSize)
        .forEach(
            position -> {
              ExprId firstExpr = childrenAttributes.get(0).get(position).exprId();
              IntStream.range(1, children.size())
                  .mapToObj(childIndex -> childrenAttributes.get(childIndex).get(position))
                  .forEach(attr -> traverseExpression(attr, firstExpr, builder));
            });
  }
}
