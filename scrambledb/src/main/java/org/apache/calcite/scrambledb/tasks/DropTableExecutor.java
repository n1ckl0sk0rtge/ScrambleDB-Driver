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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.DropTable;
import org.apache.calcite.scrambledb.ScrambledbUtil;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.apache.calcite.util.Pair;

import static org.apache.calcite.util.Static.RESOURCE;


public class DropTableExecutor {

  public static void execute(SqlDropObject drop,
      CalcitePrepare.Context context) {

    final Pair<CalciteSchema, String> pair = ScrambledbUtil.schema(context, false, drop.name);

    CalciteSchema schema = pair.left;
    String objectName = pair.right;
    assert objectName != null;

    switch (drop.getKind()) {
    case DROP_TABLE:
      boolean existed = schema != null
          && schema.schema.getTable(drop.name.toString()) != null;

      if (existed) {
        DropTable dropTableSchema = (DropTable) pair.left.schema;
        dropTableSchema.dropTable(drop.name.toString());
        dropTableSchema.reloadTablesIntoSchema();

      } else if (!drop.ifExists) {
        throw SqlUtil.newContextException(drop.name.getParserPosition(),
            RESOURCE.tableNotFound(objectName));
      }
      break;
    case OTHER_DDL:
    default:
      throw new AssertionError(drop.getKind());
    }
  }

}
