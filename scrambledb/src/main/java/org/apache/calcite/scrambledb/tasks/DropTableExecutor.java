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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.DropTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableColumn;
import org.apache.calcite.scrambledb.ScrambledbUtil;
import org.apache.calcite.scrambledb.parser.SqlCreateTable;
import org.apache.calcite.sql.SqlDrop;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;


public class DropTableExecutor {

  private final SqlDropObject drop;
  private final String name;
  private final CalcitePrepare.Context context;
  private final CalciteSchema schema;

  public DropTableExecutor(SqlDropObject drop, CalcitePrepare.Context context) {
    this.drop = drop;
    this.context = context;

    final Pair<CalciteSchema, String> pair = ScrambledbUtil.schema(context, true, drop.name);
    assert pair.left != null;
    assert pair.right != null;

    this.name = pair.right;
    this.schema = pair.left;
  }

  public List<String> getRootTableColumnsNames() {
    Table rootTable = schema.schema.getTable(name);
    assert rootTable != null;
    List<RelDataTypeField> fields =
        rootTable.getRowType(context.getTypeFactory()).getFieldList();

    ArrayList<String> names = new ArrayList<String>();
    for (RelDataTypeField field : fields) {
      names.add(field.getName());
    }

    return names;
  }

  public void executeWith(String name) {
    switch (drop.getKind()) {
    case DROP_TABLE:
      boolean existed = schema != null
          && schema.schema.getTable(name) != null;

      if (existed) {
        DropTable dropTableSchema = (DropTable) schema.schema;
        dropTableSchema.dropTable(name);
        dropTableSchema.reloadTablesIntoSchema();

      } else if (!drop.ifExists) {
        throw SqlUtil.newContextException(SqlParserPos.ZERO,
            RESOURCE.tableNotFound(name));
      }
      break;
    case OTHER_DDL:
    default:
      throw new AssertionError(drop.getKind());
    }
  }

  public void execute() {
    switch (drop.getKind()) {
    case DROP_TABLE:
      boolean existed = schema != null
          && schema.schema.getTable(drop.name.toString()) != null;

      if (existed) {
        DropTable dropTableSchema = (DropTable) schema.schema;
        dropTableSchema.dropTable(drop.name.toString());
        dropTableSchema.reloadTablesIntoSchema();

      } else if (!drop.ifExists) {
        throw SqlUtil.newContextException(drop.name.getParserPosition(),
            RESOURCE.tableNotFound(name));
      }
      break;
    case OTHER_DDL:
    default:
      throw new AssertionError(drop.getKind());
    }
  }

  public String getName() {
    return name;
  }
}
