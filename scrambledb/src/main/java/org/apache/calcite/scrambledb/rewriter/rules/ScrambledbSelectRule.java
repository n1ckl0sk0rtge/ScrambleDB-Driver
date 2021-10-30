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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.scrambledb.ScrambledbExecutor;
import org.apache.calcite.scrambledb.ScrambledbUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.SqlRewriterRule;

import java.util.ArrayList;
import java.util.List;

public class ScrambledbSelectRule implements SqlRewriterRule  {

  @Override
  public RelNode apply(RelNode node, CalcitePrepare.Context context) {
    /*
     * LogicalAggregate(group=[{}], EXPR$0=[SUM($0)])
     *   LogicalProject(I=[$0])
     *     JdbcTableScan(table=[[adhoc, T]])
     *
     */
    JdbcTableScan scan =
        (JdbcTableScan) ScrambledbUtil.contains(node, JdbcTableScan.class);
    assert scan != null;
    String tableName = scan.jdbcTable.jdbcTableName;
    List<RelDataTypeField> columnDataTypeFields = scan.getTable().getRowType().getFieldList();

    CalciteSchema schema = ScrambledbUtil.schema(context, true);
    // if Calcite is connected to a data source a schema should exist.
    // if not, an error would raise lines before.
    assert schema != null;

    List<JdbcTableScan> subTables = new ArrayList<JdbcTableScan>();

    for (RelDataTypeField field : columnDataTypeFields){
      String columnName = field.getName();
      String subTableName =
          ScrambledbExecutor.config.createSubtableString(tableName, columnName);
      JdbcTable subTable = (JdbcTable) schema.schema.getTable(subTableName);
      // this table should exist, because it was self created by create table
      // and there are no other operations allowed that delete sub-tables.
      assert subTable != null;
      // collect subtables
      subTables
          .add(getTableScan(
              context,
              schema.plus(),
              scan,
              subTable,
              subTableName));
    }

    final FrameworkConfig config = Frameworks.newConfigBuilder()
        .defaultSchema(schema.plus())
        .build();
    RelBuilder builder = RelBuilder.create(config);
    builder
        .pushAll(subTables)
        .join(JoinRelType.INNER,
            ScrambledbExecutor.config.getLinkerName());

    LogicalJoin join = (LogicalJoin) builder.build();

    RexNode linkerReference = new RexInputRef(1,
        join.getLeft().getRowType().getFieldList().get(1).getType());

    RexNode valueReference = new RexInputRef(3,
        join.getRight().getRowType().getFieldList().get(1).getType());

    List<RexNode> newProjects = ImmutableList.<RexNode>builder()
        .add(linkerReference)
        .add(valueReference)
        .build();

    RelDataTypeFactory.Builder relDataTypeBuilder =
        new RelDataTypeFactory.Builder(context.getTypeFactory());
    relDataTypeBuilder.add(join.getLeft().getRowType().getFieldList().get(1))
        .add(join.getRight().getRowType().getFieldList().get(1));

    LogicalProject p = LogicalProject.create(join,
        join.getHints(),
        newProjects,
        relDataTypeBuilder.build());

    System.out.println(p.explain());

    return p;
  }

  @Override
  public boolean isApplicable(RelNode node, SqlKind kind) {
    return kind == SqlKind.SELECT &&
        ScrambledbUtil.contains(node, JdbcTableScan.class) != null;
  }

  private JdbcTableScan getTableScan(
      CalcitePrepare.Context context,
      SchemaPlus plus,
      JdbcTableScan scan,
      JdbcTable subTable,
      String tableName) {
    // adhoc value
    String adhoc = scan.getTable().getQualifiedName().get(0);
    // Build RelDataType from field
    RelDataTypeFactory.Builder relDataTypeBuilder =
        new RelDataTypeFactory.Builder(context.getTypeFactory());
    relDataTypeBuilder
        .addAll(subTable.getRowType(context.getTypeFactory())
            .getFieldList());
    // define the new table from dataType
    RelOptTable newTableDefinition = RelOptTableImpl.create(
        scan.getTable().getRelOptSchema(),
        relDataTypeBuilder.build(),
        ImmutableList.of(
            //adhoc
            adhoc,
            // T_<column>
            tableName),
        subTable,
        subTable.getExpression(
            plus,
            tableName,
            AbstractQueryableTable.class)
    );
    /*
     * Define the new JdbcTableScan
     */
    // Define the JdbcConvention
    JdbcConvention newJdbcConvention = JdbcConvention.of(
        scan.jdbcTable.jdbcSchema.dialect,
        subTable.getExpression(
            plus,
            tableName,
            AbstractQueryableTable.class),
        adhoc + tableName
    );
    // Define new JdbcTableScan from JdbcConvention
    return new JdbcTableScan(
        scan.getCluster(),
        scan.getHints(),
        newTableDefinition,
        subTable,
        newJdbcConvention
    );

    /*List<RexNode> refs = new ArrayList<RexNode>();

    int counter = 0;
    for (RelDataTypeField field : newScan.getRowType().getFieldList()) {
      refs.add(new RexInputRef(counter, field.getType()));
      counter++;
    }

    List<RexNode> newProjects = ImmutableList.<RexNode>builder()
        .addAll(refs)
        .build();

    // Build RelDataType from field
    relDataTypeBuilder =
        new RelDataTypeFactory.Builder(context.getTypeFactory());
    relDataTypeBuilder
        .addAll(newScan.getRowType().getFieldList());

    return LogicalProject.create(
        newScan,
        ImmutableList.of(),
        newProjects,
        relDataTypeBuilder.build()
    );*/
  }

}
