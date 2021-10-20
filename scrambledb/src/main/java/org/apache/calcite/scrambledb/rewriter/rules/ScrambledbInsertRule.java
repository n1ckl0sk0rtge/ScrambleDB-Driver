/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.scrambledb.rewriter.rules;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRelImplementor;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelImplementor;
import org.apache.calcite.plan.RelOptQuery;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.scrambledb.ScrambledbConfig;
import org.apache.calcite.scrambledb.ScrambledbExecutor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.SqlRewriterRule;

import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ScrambledbInsertRule implements SqlRewriterRule {

  @Override
  public RelNode apply(RelNode node, CalcitePrepare.Context context) {
    /*
    * Structure of an insert-relational expression:
    *
    * - Without column specification
    *
    *   LogicalTableModify(table=[[adhoc, T]], operation=[INSERT], flattened=[false])
    *     LogicalValues(tuples=[[{ 1, 'hello' }]])
    *
    *   TRANSFORM TO
    *
    *   LogicalTableModify(table=[[adhoc, T]], operation=[INSERT], flattened=[false])
    *    LogicalValues(tuples=[[{<unlinkable_pseudonym>, 1, 'hello' }]])
    *
    * - With column specification
    *
    *   LogicalTableModify(table=[[adhoc, T]], operation=[INSERT], flattened=[false])
    *     LogicalProject(I=[$0], M=[$1])
    *       LogicalValues(tuples=[[{ 1, 'hello' }]])
    *
    *   TRANSFORM TO
    *
    *   LogicalTableModify(table=[[adhoc, T]], operation=[INSERT], flattened=[false])
    *     LogicalProject(linkerID=[$0], I=[$1], M=[$2])
    *       LogicalValues(tuples=[[{<unlinkable_pseudonym>, 1, 'hello' }]])
    *
    */

    LogicalValues logicalValues = containsLogicalValue(node);
    assert logicalValues != null;
    ImmutableList<ImmutableList<RexLiteral>> tuples = logicalValues.getTuples();

    String CRYPTO_VALUE = "xbjsak8123";
    RexBuilder rexBuilder = new RexBuilder(context.getTypeFactory());
    RexLiteral rex = rexBuilder.makeLiteral(CRYPTO_VALUE);

    ArrayList<ImmutableList<RexLiteral>> newTupleList = new ArrayList<>();
    for (int i = 0; i < tuples.size(); i++) {
      ArrayList<RexLiteral> line = new ArrayList<>();
      line.add(rex);
      for (int j = 0; j < tuples.get(i).size(); j++) {
        line.add(tuples.get(i).get(j));
      }
      newTupleList.add(ImmutableList.copyOf(line));
    }
    ImmutableList<ImmutableList<RexLiteral>> newTuples = ImmutableList.copyOf(newTupleList);

    RelDataTypeFactory.Builder relDataTypeBuilder =
        new RelDataTypeFactory.Builder(context.getTypeFactory());
    relDataTypeBuilder
        .addAll(ScrambledbExecutor.config.getLinkerRelDataTypeField())
        .addAll(logicalValues.getRowType().getFieldList());

    logicalValues = new LogicalValues(
        logicalValues.getCluster(),
        logicalValues.getCluster().traitSetOf(Convention.NONE),
        relDataTypeBuilder.build(), newTuples);

    LogicalProject logicalProject = containsLogicalProject(node);

    if (logicalProject != null) {
      List<RexNode> projects = logicalProject.getProjects();

      RexNode linkerColumn = projects.get(0);
      RexNode newReference = new RexInputRef(0, linkerColumn.getType());

      List<RexInputRef> incrementedReferences =
          tryIncrementReferences(projects.subList(1, projects.size()));
      assert incrementedReferences != null;

      List<RexNode> newProjects = ImmutableList.<RexNode>builder()
          .add(newReference)
          .addAll(incrementedReferences)
          .build();

      logicalProject = new LogicalProject(
          logicalProject.getCluster(),
          logicalProject.getTraitSet(),
          logicalProject.getHints(),
          logicalValues,
          newProjects,
          logicalProject.getRowType());

    } else {
      // TODO: Create a default logical project that will be inserted by default
    }

    LogicalTableModify insertRelQuery = (LogicalTableModify) contains(node, LogicalTableModify.class);

    node = new LogicalTableModify(
        insertRelQuery.getCluster(), //TODO
        insertRelQuery.getTraitSet(),
        insertRelQuery.getTable(),
        insertRelQuery.getCatalogReader(),
        logicalProject,
        insertRelQuery.getOperation(),
        insertRelQuery.getUpdateColumnList(),
        insertRelQuery.getSourceExpressionList(),
        insertRelQuery.isFlattened());

    return node;
  }

  @Override
  public boolean isApplicable(SqlKind kind) {
    return kind == SqlKind.INSERT;
  }

  private @Nullable List<RexInputRef> tryIncrementReferences(List<RexNode> rexNodes) {
    List<RexInputRef> newRefsList = new ArrayList<>();

    for (RexNode rex : rexNodes) {
      if (rex instanceof RexInputRef){
        RexInputRef ref = (RexInputRef) rex;
        newRefsList.add(new RexInputRef( ref.getIndex() + 1, ref.getType()));
      } else {
        return null;
      }
    }
    return ImmutableList.copyOf(newRefsList);
  }

  private @Nullable <T> RelNode contains(RelNode node, Class<T> type) {
    if (node.getClass() == type) {
      return node;
    }

    for (int i = 0; i < node.getInputs().size(); i++) {
      return contains(node.getInputs().get(i), type);
    }
    return null;
  }

  private @Nullable LogicalValues containsLogicalValue(RelNode node) {
      return (LogicalValues) contains(node, LogicalValues.class);
  }

  private @Nullable LogicalProject containsLogicalProject(RelNode node) {
    return (LogicalProject) contains(node, LogicalProject.class);
  }

}
