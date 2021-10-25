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

package org.apache.calcite.scrambledb.tasks;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.CreateTable;
import org.apache.calcite.scrambledb.ScrambledbUtil;
import org.apache.calcite.scrambledb.parser.SqlCreateTable;
import org.apache.calcite.schema.TableColumn;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Pair;

import static org.apache.calcite.util.Static.RESOURCE;

public class CreateTableExecutor {

  private final SqlCreateTable create;
  private final String name;
  public  ImmutableList<TableColumn> columns;
  private final CalcitePrepare.Context context;
  private final JavaTypeFactory typeFactory;
  private final CalciteSchema schema;

  public CreateTableExecutor(SqlCreateTable create, CalcitePrepare.Context context) {
    this.create = create;
    this.context = context;
    this.typeFactory = context.getTypeFactory();

    final Pair<CalciteSchema, String> pair = ScrambledbUtil.schema(context, true, create.name);
    assert pair.left != null;
    assert pair.right != null;

    this.name = pair.right;
    this.schema = pair.left;
    load();
  }

  private void load() {
    if (create.columnList == null) {
      throw SqlUtil.newContextException(create.name.getParserPosition(),
          RESOURCE.createTableRequiresColumnList());
    }

    final ImmutableList.Builder<TableColumn> columnBuilder = ImmutableList.builder();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    final RelDataTypeFactory.Builder storedBuilder = typeFactory.builder();
    final SqlValidator validator = ScrambledbUtil.validator(context, true);

    for (Ord<SqlNode> c : Ord.zip(create.columnList)) {
      if (c.e instanceof SqlColumnDeclaration) {
        final SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) c.e;
        final RelDataType type = columnDeclaration.dataType.deriveType(validator, true);
        builder.add(columnDeclaration.name.getSimple(), type);

        if (columnDeclaration.strategy != ColumnStrategy.VIRTUAL) {
          storedBuilder.add(columnDeclaration.name.getSimple(), type);
        }
        columnBuilder.add(TableColumn.of(
            columnDeclaration.name.toString(),
            columnDeclaration.expression,
            type,
            columnDeclaration.strategy));
      } else {
        throw new AssertionError(c.e.getClass());
      }
    }
    this.columns = columnBuilder.build();
  }

  public String getName() {
    return name;
  }

  public void executeWith(String name, ImmutableList<TableColumn> columns) throws ScrambledbUtil.CreateTableFunctionalityIsNotPartOfSchema {
    if (schema.plus().getTable(this.name) != null) {
      // Table exists.
      if (!create.getReplace()) {
        // They did not specify IF NOT EXISTS, so give error.
        throw SqlUtil.newContextException(create.name.getParserPosition(),
            RESOURCE.tableExists(this.name));
      }
    } else {
      if (schema.schema instanceof CreateTable) {
        CreateTable createTableSchema = (CreateTable) schema.schema;
        createTableSchema.createTable(
            name,
            columns);
        createTableSchema.reloadTablesIntoSchema();
      } else {
        throw new ScrambledbUtil.CreateTableFunctionalityIsNotPartOfSchema(
            "Create table functionality is not part of schema");
      }
    }
  }

  public void execute() throws ScrambledbUtil.CreateTableFunctionalityIsNotPartOfSchema {
    if (schema.plus().getTable(name) != null) {
      // Table exists.
      if (!create.getReplace()) {
        // They did not specify IF NOT EXISTS, so give error.
        throw SqlUtil.newContextException(create.name.getParserPosition(),
            RESOURCE.tableExists(name));
      }
    } else {
      if (schema.schema instanceof CreateTable) {
        CreateTable createTableSchema = (CreateTable) schema.schema;
        createTableSchema.createTable(
            create.name.toString(),
            this.columns);
        createTableSchema.reloadTablesIntoSchema();
      } else {
        throw new ScrambledbUtil.CreateTableFunctionalityIsNotPartOfSchema(
            "Create table functionality is not part of schema");
      }
    }
  }

}
