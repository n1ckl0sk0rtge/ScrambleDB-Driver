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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.scrambledb.ScrambledbErrors;
import org.apache.calcite.scrambledb.ScrambledbExecutor;
import org.apache.calcite.scrambledb.ScrambledbUtil;
import org.apache.calcite.scrambledb.rest.ScrambledbRestClient;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.SqlRewriterRule;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rule for rewriting insert statements.
 */
public class ScrambledbInsertRule implements SqlRewriterRule {

  /**
   * Explanation.
   *
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
   *    LogicalValues(tuples=[[{unlinkable_pseudonym, 1 }]])
   *
   *   LogicalTableModify(table=[[adhoc, T_M]], operation=[INSERT], flattened=[false])
   *    LogicalValues(tuples=[[{unlinkable_pseudonym, 'hello' }]])
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
   *       LogicalValues(tuples=[[{unlinkable_pseudonym, 1 }]])
   *
   *   LogicalTableModify(table=[[adhoc, T_M]], operation=[INSERT], flattened=[false])
   *     LogicalProject(linkerID=[$0], M=[$1])
   *       LogicalValues(tuples=[[{unlinkable_pseudonym, 'hello' }]])
   *
   */
  @Override public RelNode apply(RelNode node, CalcitePrepare.Context context)
      throws ScrambledbErrors.RewriteInsertRuleError, SQLException {

    LogicalTableModify logicalTableModify =
        (LogicalTableModify) ScrambledbUtil.contains(node, LogicalTableModify.class);
    assert logicalTableModify != null;
    int indexOfPrimaryKey = getPrimaryKeyColumn(logicalTableModify);

    RelOptTable tableDefinition = logicalTableModify.getTable();
    List<String> qualifiedName = tableDefinition.getQualifiedName();
    assert qualifiedName.get(1) != null;
    String rootTableName = qualifiedName.get(1);

    LogicalValues logicalValues =
        (LogicalValues) ScrambledbUtil.contains(node, LogicalValues.class);
    assert logicalValues != null;
    ImmutableList<ImmutableList<RexLiteral>> tuples = logicalValues.getTuples();

    LogicalProject logicalProject =
        (LogicalProject) ScrambledbUtil.contains(node, LogicalProject.class);

    RelNode newNode = node;

    // Connect to converter
    ScrambledbRestClient client = new ScrambledbRestClient();

    for (int i = 0; i < tuples.size(); i++) {
      // new line in the table
      RexLiteral referenceKey;
      if (indexOfPrimaryKey > -1) {
        referenceKey = tuples.get(i).get(indexOfPrimaryKey);
      } else {
        // default: first column equals primnary key
        referenceKey = tuples.get(i).get(0);
      }
      List<RexLiteral> linker = getLinkers(client, referenceKey, tuples.get(i).size(), context);
      for (int j = 0; j < tuples.get(i).size(); j++) {
        // new value in table (in column)
        RelDataTypeField currentValueRelDataType = logicalValues.getRowType().getFieldList().get(j);
        // define new type list
        RelDataTypeFactory.Builder relDataTypeBuilder =
            new RelDataTypeFactory.Builder(context.getTypeFactory());
        relDataTypeBuilder
            // add linker type from config
            .addAll(ScrambledbExecutor.config.getLinkerRelDataTypeField())
            // add value type from given logical value
            .add(currentValueRelDataType);
        // define values
        LogicalValues newLogicalValues = new LogicalValues(
            logicalValues.getCluster(),
            logicalValues.getCluster().traitSetOf(Convention.NONE),
            relDataTypeBuilder.build(),
            ImmutableList.<ImmutableList<RexLiteral>>builder()
                .add(ImmutableList.<RexLiteral>builder()
                    .add(linker.get(j))
                    .add(tuples.get(i).get(j))
                    .build())
                .build());

        LogicalProject newLogicalProject;

        if (logicalProject != null) {
          List<RexNode> projects = logicalProject.getProjects();

          // define Reference Node
          RexNode linkerReference = new RexInputRef(0,
              ScrambledbExecutor.config.getLinkerRelDataType());
          // increment value reference by 1
          RexNode valueReference = incrementReferences(projects.get(j));
          if (valueReference == null) {
            throw new ScrambledbErrors.RewriteInsertRuleError(node);
          }

          List<RexNode> newProjects = ImmutableList.<RexNode>builder()
              .add(linkerReference)
              // get the j project element
              .add(valueReference)
              .build();

          newLogicalProject = LogicalProject.create(
              newLogicalValues,
              logicalProject.getHints(),
              newProjects,
              newLogicalValues.getRowType().getFieldNames());

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

          newLogicalProject = LogicalProject.create(
              newLogicalValues,
              ImmutableList.of(),
              newProjects,
              newLogicalValues.getRowType().getFieldNames());

        }

        CalciteSchema schema = ScrambledbUtil.schema(context, true);
        // if Calcite is connected to a data source a schema should exist.
        // if not, an error would raise lines before.
        assert schema != null;

        String columnName = newLogicalValues.getRowType().getFieldNames().get(1);
        String subTableName = ScrambledbExecutor
            .config.createSubtableString(rootTableName, columnName);

        Table table = schema.schema.getTable(subTableName);
        // this table should exist, because it was self created by create table
        // and there are no other operations allowed that delete sub-tables.
        assert table != null;

        RelOptTable newTableDefinition = RelOptTableImpl.create(
            tableDefinition.getRelOptSchema(),
            newLogicalValues.getRowType(),
            ImmutableList.of(
                //adhoc
                newLogicalValues.getRowType().getFieldNames().get(0),
                // T_<column>
                subTableName),
            table,
            tableDefinition.getExpression(Queryable.class));

        newNode = new LogicalTableModify(
            logicalTableModify.getCluster(),
            logicalTableModify.getTraitSet(),
            newTableDefinition,
            logicalTableModify.getCatalogReader(),
            newLogicalProject,
            logicalTableModify.getOperation(),
            logicalTableModify.getUpdateColumnList(),
            logicalTableModify.getSourceExpressionList(),
            logicalTableModify.isFlattened());

        // Run all underlying sql queries here and only return the last
        // query to run on the "origin" way
        if (!(i == tuples.size() - 1 && j == tuples.get(i).size() - 1)) {
          PreparedStatement statement =
              context.getRelRunner().prepareStatement(newNode);
          statement.execute();
          statement.close();
        }

      }
    }

    // Close Rest connection
    client.close();

    return newNode;
  }

  @Override public boolean isApplicable(@NonNull RelNode node, @NonNull SqlKind kind) {
    return kind == SqlKind.INSERT
        && ScrambledbUtil.contains(node, LogicalTableModify.class) != null;
  }

  private @Nullable RexNode incrementReferences(RexNode rexNode) {
    if (rexNode instanceof RexInputRef) {
      RexInputRef ref = (RexInputRef) rexNode;
      return new RexInputRef(ref.getIndex() + 1, ref.getType());
    }
    return null;
  }

  private int getPrimaryKeyColumn(LogicalTableModify table) {
    for (RelDataTypeField field : table.getTable().getRowType().getFieldList()) {
      SqlCollation collation = field.getValue().getCollation();
      if (collation != null) {
        if (collation.getCollationName().contains("primary")) {
          return field.getIndex();
        }
      }
    }
    return -1;
  }

  private List<RexLiteral> getLinkers(ScrambledbRestClient client, RexLiteral primaryKey, int count, CalcitePrepare.Context context) {
    List<String> input = Collections.nCopies(count, primaryKey.toString().replaceAll("[']*", ""));
    List<String> pseudonyms = client.getPseudonyms(input);

    return pseudonyms.stream().map(
        pse -> {
          RexBuilder rexBuilder = new RexBuilder(context.getTypeFactory());
          return rexBuilder.makeLiteral(pse);
        }
    ).collect(Collectors.toList());
  }

}
