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

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.adapter.jdbc.JdbcTableScan;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.scrambledb.ScrambledbExecutor;
import org.apache.calcite.scrambledb.ScrambledbUtil;
import org.apache.calcite.sql.SqlDialect;
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
     *
     *
    JdbcTableScan scan = (JdbcTableScan) ScrambledbUtil.contains(node, JdbcTableScan.class);
    assert scan != null;
    System.out.println(node.explain());

    CalciteSchema schema = ScrambledbUtil.schema(context, true);
    // if Calcite is connected to a data source a schema should exist.
    // if not, an error would raise lines before.
    assert schema != null;


    JdbcTable table = (JdbcTable) schema.schema.getTable("T_NUMBERS");
    // this table should exist, because it was self created by create table
    // and there are no other operations allowed that delete sub-tables.
    assert table != null;

    RelDataTypeFactory.Builder relDataTypeBuilder =
        new RelDataTypeFactory.Builder(context.getTypeFactory());
    relDataTypeBuilder
        .add(table.getRowType(context.getTypeFactory()).getFieldList().get(1));

    RelOptTable newTableDefinition = RelOptTableImpl.create(
        scan.getTable().getRelOptSchema(),
        relDataTypeBuilder.build(),
        ImmutableList.of(
            //adhoc
            scan.getTable().getQualifiedName().get(0),
            // T_<column>
            "T_NUMBERS"),
        table,
        scan.getTable().getExpression(Queryable.class)
    );

    JdbcConvention con = JdbcConvention.of(
        scan.jdbcTable.jdbcSchema.dialect,
        scan.jdbcTable.getExpression(
            context.getRootSchema().plus(),
            "T_NUMBERS",
            Queryable.class
        ),
        "T_NUMBERS"
    );

    JdbcTableScan newScan = new JdbcTableScan(
        scan.getCluster(),
        scan.getHints(),
        newTableDefinition,
        table,
        con
    );

    LogicalProject logicalProject =
        (LogicalProject) ScrambledbUtil.contains(node, LogicalProject.class);
    assert logicalProject != null;

    RexNode value = new RexInputRef(0,
        logicalProject.getRowType().getFieldList().get(0).getType());

    List<RexNode> newProjects = ImmutableList.<RexNode>builder()
        .add(value)
        .build();

    LogicalProject newLogicalProject = LogicalProject.create(
        newScan,
        ImmutableList.of(),
        newProjects,
        ImmutableList.of(logicalProject.getRowType().getFieldNames().get(0))
    );

    System.out.println(newLogicalProject.explain());

    //return newLogicalProject;

     */
    return node;
  }

  @Override
  public boolean isApplicable(RelNode node, SqlKind kind) {
    return kind == SqlKind.SELECT &&
        ScrambledbUtil.contains(node, JdbcTableScan.class) != null;
  }

}
