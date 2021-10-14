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
import org.apache.calcite.scrambledb.parser.TableColumn;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

public class CreateTableExecutor {

  public static void execute(SqlCreateTable create,
      CalcitePrepare.Context context) throws
      ScrambledbUtil.CreateTableFunctionalityIsNotPartOfSchema {

    final Pair<CalciteSchema, String> pair = ScrambledbUtil.schema(context, true, create.name);
    final JavaTypeFactory typeFactory = context.getTypeFactory();
    final RelDataType queryRowType;

    assert pair.left != null;
    assert pair.right != null;

    queryRowType = null;

    final List<SqlNode> columnList;

    if (create.columnList != null) {
      columnList = create.columnList;
    } else {
      if (queryRowType == null) {
        // "CREATE TABLE t" is invalid; because there is no "AS query" we need
        // a list of column names and types, "CREATE TABLE t (INT c)".
        throw SqlUtil.newContextException(create.name.getParserPosition(),
            RESOURCE.createTableRequiresColumnList());
      }
      columnList = new ArrayList<>();
      for (String name : queryRowType.getFieldNames()) {
        columnList.add(new SqlIdentifier(name, SqlParserPos.ZERO));
      }
    }

    final ImmutableList.Builder<TableColumn> b = ImmutableList.builder();
    final RelDataTypeFactory.Builder builder = typeFactory.builder();
    final RelDataTypeFactory.Builder storedBuilder = typeFactory.builder();
    final SqlValidator validator = ScrambledbUtil.validator(context, true);

    for (Ord<SqlNode> c : Ord.zip(columnList)) {
      if (c.e instanceof SqlColumnDeclaration) {

        final SqlColumnDeclaration d = (SqlColumnDeclaration) c.e;
        final RelDataType type = d.dataType.deriveType(validator, true);

        builder.add(d.name.getSimple(), type);

        if (d.strategy != ColumnStrategy.VIRTUAL) {
          storedBuilder.add(d.name.getSimple(), type);
        }

        b.add(TableColumn.of(d.expression, type, d.strategy));

      } else if (c.e instanceof SqlIdentifier) {

        final SqlIdentifier id = (SqlIdentifier) c.e;

        if (queryRowType == null) {
          throw SqlUtil.newContextException(id.getParserPosition(),
              RESOURCE.createTableRequiresColumnTypes(id.getSimple()));
        }

        final RelDataTypeField f = queryRowType.getFieldList().get(c.i);
        final ColumnStrategy strategy = f.getType().isNullable()
            ? ColumnStrategy.NULLABLE
            : ColumnStrategy.NOT_NULLABLE;

        b.add(TableColumn.of(c.e, f.getType(), strategy));
        builder.add(id.getSimple(), f.getType());
        storedBuilder.add(id.getSimple(), f.getType());
      } else {
        throw new AssertionError(c.e.getClass());
      }
    }

    final RelDataType rowType = builder.build();


    if (pair.left.plus().getTable(pair.right) != null) {
      // Table exists.
      if (create.ifNotExists) {
        return;
      }
      if (!create.getReplace()) {
        // They did not specify IF NOT EXISTS, so give error.
        throw SqlUtil.newContextException(create.name.getParserPosition(),
            RESOURCE.tableExists(pair.right));
      }
    }

    if (pair.left.schema instanceof CreateTable) {

      CreateTable createTableSchema = (CreateTable) pair.left.schema;

      createTableSchema.createTable(
          create.name.toString(),
          RelDataTypeImpl.proto(rowType));

      createTableSchema.reloadTablesIntoSchema();
    } else {
      throw new ScrambledbUtil.CreateTableFunctionalityIsNotPartOfSchema(
          "Create table functionality is not part of schema");
    }

  }
}
