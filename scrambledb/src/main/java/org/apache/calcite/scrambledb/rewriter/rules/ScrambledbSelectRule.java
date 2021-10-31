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

import com.sun.jmx.remote.internal.ArrayQueue;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.scrambledb.ScrambledbErrors;
import org.apache.calcite.scrambledb.ScrambledbExecutor;
import org.apache.calcite.scrambledb.ScrambledbUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.SqlRewriterRule;

import java.util.*;

public class ScrambledbSelectRule implements SqlRewriterRule  {

  @Override
  public RelNode apply(RelNode node, CalcitePrepare.Context context) {
    /*
     * LogicalProject(NAME=[$0], AGE=[$1])
     *  JdbcTableScan(table=[[adhoc, CUSTOMER]])
     *
     * transferred to
     *
     * LogicalProject(NAME=[$0], AGE=[$1])
     *  LogicalProject(NAME=[$1], AGE=[$3])
     *    LogicalJoin(condition=[=($0, $2)], joinType=[full])
     *      JdbcTableScan(table=[[adhoc, CUSTOMER_NAME]])
     *       JdbcTableScan(table=[[adhoc, CUSTOMER_AGE]])
     *
     */

    CalciteSchema schema = ScrambledbUtil.schema(context, true);
    // if Calcite is connected to a data source a schema should exist.
    // if not, an error would raise lines before.
    assert schema != null;
    // get all instances of JdbcTableScan from the given AST
    List<RelNode> scans = ScrambledbUtil.containsMultiple(node, JdbcTableScan.class);
    // create the scrambledb JdbcScan from the given JdbcTableScan and store them in a stack
    Stack<RelNode> replacements = new Stack<>();
    for (RelNode scan : scans) {
      replacements.add(
          getScrambledbTableScan( (JdbcTableScan) scan, schema, context));
    }
    // replace each default JdbcTableScan with a node for scanning the scrambled tables
    try {
      return replaceJdbcTableScanWithScrambledbTableScan(node, replacements, new Stack<>());
    } catch (Exception e) {
      e.printStackTrace();
    }
    // return unchanged node as default
    return node;
  }

  @Override
  public boolean isApplicable(RelNode node, SqlKind kind) {
    return kind == SqlKind.SELECT &&
        ScrambledbUtil.contains(node, JdbcTableScan.class) != null;
  }

  private RelNode getScrambledbTableScan(
      JdbcTableScan defaultJdbcTableScan,
      CalciteSchema schema,
      CalcitePrepare.Context context) {
    // get the name of the rootTable
    String tableName = defaultJdbcTableScan.jdbcTable.jdbcTableName;
    // get the relDataTypeFields from the rootTable
    List<RelDataTypeField> columnDataTypeFields =
        defaultJdbcTableScan.getTable().getRowType().getFieldList();
    // create a Stack of JdbcTableScans. This stack will contain all necessary table scans to get
    // the whole rootTable. Because all columns (values) are scrambled over different tables, we
    // have to perform a jdbCTableScan for each subtable (column)
    Stack<JdbcTableScan> subTables = new Stack<>();
    // relevant values = actual values in the database, not the linker
    // both list contains meta information for creating a join and a projection over the
    // scrambled tables. The linker metadata will be ignored here.
    List<RexNode> relevantValuesReference = new ArrayList<>();
    List<RelDataTypeField> relevantValuesDataField = new ArrayList<>();
    // start with 1, because each subtable contains in the first column the linker value. And then
    // increment the counter by 2.
    int counter = 1;
    for (RelDataTypeField field : columnDataTypeFields){
      String columnName = field.getName();
      // generating the subtable name by using the global config
      String subTableName =
          ScrambledbExecutor.config.createSubtableString(tableName, columnName);
      // get subtable in schema
      JdbcTable subTable = (JdbcTable) schema.schema.getTable(subTableName);
      // this table should exist, because it was self created by create table
      // and there are no other operations allowed that delete sub-tables.
      assert subTable != null;
      // collect subtables
      subTables
          .add(getTableScan(
              context,
              schema.plus(),
              defaultJdbcTableScan,
              subTable,
              subTableName));
      // create references to the relevant values for the projection
      RelDataTypeField relevantField = subTable.getRowType(context.getTypeFactory())
          // 1 = because 0 is always the linker and 1 always the actual value
          .getFieldList().get(1);
      relevantValuesReference.add(new RexInputRef(counter, relevantField.getType()));
      // add values type
      relevantValuesDataField.add(relevantField);
      // increment counter with 2
      counter += 2;
    }
    // create the join over the scrambled tables
    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(schema.plus())
        .build();
    RelBuilder builder = RelBuilder.create(config);
    // start with the first right join element
    RelNode rightJoinElement = subTables.pop();
    // join all subtables together -> creating the relational expr for that
    LogicalJoin join = (LogicalJoin) join(subTables, rightJoinElement, builder);
    // define a projection over the joined tables.
    // the join would look like this:
    //
    //  ______________________________________
    //  | linker | value1 | linker | value 2 |
    //
    //  We only want to get the value columns, so we add a projection on those columns on top
    List<RexNode> newProjects = ImmutableList.<RexNode>builder()
        .addAll(relevantValuesReference)
        .build();

    RelDataTypeFactory.Builder relDataTypeBuilder =
        new RelDataTypeFactory.Builder(context.getTypeFactory());
    relDataTypeBuilder.addAll(relevantValuesDataField);

    return LogicalProject.create(join,
        join.getHints(),
        newProjects,
        relDataTypeBuilder.build());
  }

  private RelNode replaceJdbcTableScanWithScrambledbTableScan(
      RelNode currentNode,
      Stack<RelNode> replaceNodes,
      Stack<RelNode> relNodeCache)
      throws ScrambledbErrors.SelectReplacementError {

    if (currentNode.getClass() == JdbcTableScan.class) {
      if (replaceNodes.empty()) {
        throw new ScrambledbErrors.SelectReplacementError(currentNode, replaceNodes);
      }
      return replaceNodes.pop();
    }

    for (int i = 0; i < currentNode.getInputs().size(); i++) {
      relNodeCache.add(
          replaceJdbcTableScanWithScrambledbTableScan(
              currentNode.getInput(i),
              replaceNodes,
              relNodeCache)
      );
    }
    // Joins are of type BiRel
    if (currentNode instanceof BiRel) {
      assert relNodeCache.size() == 2;
      currentNode.replaceInput(0, relNodeCache.pop());
      currentNode.replaceInput(1, relNodeCache.pop());
    } else {
      assert currentNode instanceof SingleRel;
      assert relNodeCache.size() == 1;
      currentNode.replaceInput(0, relNodeCache.pop());
    }
    return currentNode;
  }

  private RelNode join(
      Stack<JdbcTableScan> left,
      RelNode right,
      RelBuilder builder) {
    if (left.empty()) {
      return right;
    } else {
      builder
          .push(left.pop())
          .push(right)
          .join(JoinRelType.FULL,
              ScrambledbExecutor.config.getLinkerName());
      RelNode newRight = builder.build();
      builder.clear();
      return join(left, newRight, builder);
    }
  }

  private JdbcTableScan getTableScan(
      CalcitePrepare.Context context,
      SchemaPlus schemaPlus,
      JdbcTableScan defaultJdbcTableScan,
      JdbcTable subTable,
      String subTableName) {
    // adhoc value
    String adhoc = defaultJdbcTableScan.getTable().getQualifiedName().get(0);
    // Build RelDataType from field
    RelDataTypeFactory.Builder relDataTypeBuilder =
        new RelDataTypeFactory.Builder(context.getTypeFactory());
    relDataTypeBuilder
        .addAll(subTable.getRowType(context.getTypeFactory())
            .getFieldList());
    // define the new table from dataType
    RelOptTable newTableDefinition = RelOptTableImpl.create(
        defaultJdbcTableScan.getTable().getRelOptSchema(),
        relDataTypeBuilder.build(),
        ImmutableList.of(
            //adhoc
            adhoc,
            // T_<column>
            subTableName),
        subTable,
        subTable.getExpression(
            schemaPlus,
            subTableName,
            AbstractQueryableTable.class)
    );
    /*
     * Define the new JdbcTableScan
     */
    // Define the JdbcConvention
    JdbcConvention newJdbcConvention = JdbcConvention.of(
        defaultJdbcTableScan.jdbcTable.jdbcSchema.dialect,
        subTable.getExpression(
            schemaPlus,
            subTableName,
            AbstractQueryableTable.class),
        adhoc + subTableName
    );
    // Define new JdbcTableScan from JdbcConvention
    return new JdbcTableScan(
        defaultJdbcTableScan.getCluster(),
        defaultJdbcTableScan.getHints(),
        newTableDefinition,
        subTable,
        newJdbcConvention
    );
  }

}
