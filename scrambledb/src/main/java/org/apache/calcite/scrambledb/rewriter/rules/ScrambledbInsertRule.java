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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
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
import org.apache.calcite.schema.*;
import org.apache.calcite.scrambledb.ScrambledbExecutor;
import org.apache.calcite.scrambledb.ScrambledbUtil;
import org.apache.calcite.scrambledb.rewriter.ScrambledbDistinctRelRunner;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.SqlRewriterRule;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

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
    *   LogicalTableModify(table=[[adhoc, T_I]], operation=[INSERT], flattened=[false])
    *    LogicalValues(tuples=[[{<unlinkable_pseudonym>, 1 }]])
    *
    *   LogicalTableModify(table=[[adhoc, T_M]], operation=[INSERT], flattened=[false])
    *    LogicalValues(tuples=[[{<unlinkable_pseudonym>, 'hello' }]])
    *
    *
    * - With column specification
    *
    *   LogicalTableModify(table=[[adhoc, T]], operation=[INSERT], flattened=[false])
    *     LogicalProject(I=[$0], M=[$1])
    *       LogicalValues(tuples=[[{ 1, 'hello' }]])
    *
    *   TRANSFORM TO
    *
    *   LogicalTableModify(table=[[adhoc, T_I]], operation=[INSERT], flattened=[false])
    *     LogicalProject(linkerID=[$0], I=[$1])
    *       LogicalValues(tuples=[[{<unlinkable_pseudonym>, 1 }]])
    *
    *   LogicalTableModify(table=[[adhoc, T_M]], operation=[INSERT], flattened=[false])
    *     LogicalProject(linkerID=[$0], M=[$1])
    *       LogicalValues(tuples=[[{<unlinkable_pseudonym>, 'hello' }]])
    *
    */

    LogicalTableModify logicalTableModify = (LogicalTableModify) contains(node, LogicalTableModify.class);
    assert logicalTableModify != null;
    RelOptTable tableDefinition = logicalTableModify.getTable();
    List<String> qualifiedName = tableDefinition.getQualifiedName();
    assert qualifiedName.get(1) != null;
    String rootTableName = qualifiedName.get(1);

    LogicalValues logicalValues = containsLogicalValue(node);
    assert logicalValues != null;
    ImmutableList<ImmutableList<RexLiteral>> tuples = logicalValues.getTuples();

    LogicalProject logicalProject = containsLogicalProject(node);

    RelDataTypeFactory.Builder relDataTypeBuilder =
        new RelDataTypeFactory.Builder(context.getTypeFactory());

    for (int i = 0; i < tuples.size(); i++) {
      // new line in the table
      List<RexLiteral> linker = getLinkers(tuples.size(), context);
      for (int j = 0; j < tuples.get(i).size(); j++) {
        // new value in table (in column)
        RelDataTypeField currentValueRelDataType = logicalValues.getRowType().getFieldList().get(j);
        // define new type list
        relDataTypeBuilder
            // add linker type from config
            .addAll(ScrambledbExecutor.config.getLinkerRelDataTypeField())
            // add value type from given logical value
            .add(currentValueRelDataType);
        // define values
        logicalValues = new LogicalValues(
            logicalValues.getCluster(),
            logicalValues.getCluster().traitSetOf(Convention.NONE),
            relDataTypeBuilder.build(),
            ImmutableList.<ImmutableList<RexLiteral>>builder()
                .add(ImmutableList.<RexLiteral>builder()
                    .add(linker.get(j))
                    .add(tuples.get(i).get(j))
                    .build())
                .build());

        if (logicalProject != null) {
          List<RexNode> projects = logicalProject.getProjects();

          // define Reference Node
          RexNode linkerReference = new RexInputRef(0, ScrambledbExecutor.config.getLinkerRelDataType());

          List<RexNode> newProjects = ImmutableList.<RexNode>builder()
              .add(linkerReference)
              // get the j project element
              .add(incrementReferences(projects.get(j)))
              .build();

          logicalProject = LogicalProject.create(
              logicalValues,
              logicalProject.getHints(),
              newProjects,
              logicalValues.getRowType().getFieldNames());

        } else {
          // define Reference Node
          RexNode linkerReference = new RexInputRef(0,
              ScrambledbExecutor.config.getLinkerRelDataType());

          RexNode valueReference = new RexInputRef(1,
              currentValueRelDataType.getType());

          List<RexNode> newProjects = ImmutableList.<RexNode>builder()
              .add(linkerReference)
              .add(valueReference)
              .build();

          //TODO: find out and define hints
          RelHint a = RelHint.builder("").build();

          logicalProject = LogicalProject.create(
              logicalValues,
              ImmutableList.of(a),
              newProjects,
              logicalValues.getRowType().getFieldNames());

        }

        CalciteSchema schema = ScrambledbUtil.schema(context, true);
        // if Calcite is connected to a data source a schema should exist.
        // if not, an error would raise lines before.
        assert schema != null;

        String columnName = logicalValues.getRowType().getFieldNames().get(1);
        String subTableName = ScrambledbExecutor
            .config.createSubtableString(rootTableName, columnName);

        Table table = schema.schema.getTable(subTableName);
        // this table should exist, because it was self created by create table
        // and there are no other operations allowed that delete sub-tables.
        assert table != null;

        RelOptTable to = RelOptTableImpl.create(tableDefinition.getRelOptSchema(),
            logicalValues.getRowType(),
            ImmutableList.of(
                logicalValues.getRowType().getFieldNames().get(0),
                subTableName),
            table,
            tableDefinition.getExpression(Queryable.class));

        node = new LogicalTableModify(
            logicalTableModify.getCluster(),
            logicalTableModify.getTraitSet(),
            to,
            logicalTableModify.getCatalogReader(),
            logicalProject,
            logicalTableModify.getOperation(),
            logicalTableModify.getUpdateColumnList(),
            logicalTableModify.getSourceExpressionList(),
            logicalTableModify.isFlattened());

        System.out.println(node.explain());

        //TODO: try to connect nodes to one operation
        try {
          ScrambledbDistinctRelRunner.runRelQuery(RelRoot.of(node, SqlKind.INSERT), context);
        } catch (Exception e) {

        }

      }
    }

    return node;
  }

  @Override
  public boolean isApplicable(SqlKind kind) {
    return kind == SqlKind.INSERT;
  }


  private RexNode incrementReferences(RexNode rexNode) {
    if (rexNode instanceof RexInputRef){
      RexInputRef ref = (RexInputRef) rexNode;
      return new RexInputRef( ref.getIndex() + 1, ref.getType());
    } else {
      return new RexLiteral(null, rexNode.getType(), rexNode.getType().getSqlTypeName());
    }
  }


  private List<RexLiteral> getLinkers(int count, CalcitePrepare.Context context) {
    List<RexLiteral> links = new ArrayList<>();

    //TODO: get unlinkable pseudonym here
    String CRYPTO_VALUE = "xbjsak8123";

    for (int i = 0; i < count; i ++) {
      RexBuilder rexBuilder = new RexBuilder(context.getTypeFactory());
      RexLiteral rex = rexBuilder.makeLiteral(CRYPTO_VALUE);
      links.add(rex);
    }
    return links;
  }


  private @Nullable List<RelDataTypeField> linkerColumns(String rootTable, CalcitePrepare.Context context) {
    CalciteSchema schema = ScrambledbUtil.schema(context, true);
    if (schema != null) {
      Table table = schema.schema.getTable(rootTable);
      if (table != null ) {
        return table.getRowType(context.getTypeFactory()).getFieldList();
      } else {
        //TODO: Throw schema error
        return null;
      }
    } else {
      return null;
    }
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
