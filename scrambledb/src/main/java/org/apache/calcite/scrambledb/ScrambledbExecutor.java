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
package org.apache.calcite.scrambledb;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.TableColumn;
import org.apache.calcite.scrambledb.parser.SqlCreateTable;
import org.apache.calcite.scrambledb.tasks.CreateTableExecutor;
import org.apache.calcite.scrambledb.tasks.DropTableExecutor;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.server.DdlExecutorImpl;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.scrambledb.SqlScrambledbParserImpl;
import org.apache.calcite.sql.type.BasicSqlType;

import java.io.Reader;
import java.util.List;

/** Executes ScrambleDB related commands.
 *
 * <p>Given a DDL command that is a sub-class of {@link org.apache.calcite.sql.SqlNode}, dispatches
 * the command to an appropriate {@code execute} method. For example,
 * "CREATE TABLE" ({@link SqlCreateTable}) is dispatched to
 * {@link #execute(SqlCreateTable, CalcitePrepare.Context)}. */
public class ScrambledbExecutor extends DdlExecutorImpl {

  public static final ScrambledbExecutor INSTANCE = new ScrambledbExecutor();

  public static ScrambledbConfig config = new ScrambledbConfig();

  protected ScrambledbExecutor() {  }

  /** Parser factory. */
  public static final SqlParserImplFactory PARSER_FACTORY =
      new SqlParserImplFactory() {
        @Override public SqlAbstractParserImpl getParser(Reader stream) {
          return SqlScrambledbParserImpl.FACTORY.getParser(stream);
        }

        @Override public DdlExecutor getDdlExecutor() {
          return ScrambledbExecutor.INSTANCE;
        }

      };

  /** Executes a {@code CREATE TABLE} command. */
  public void execute(SqlCreateTable create,
      CalcitePrepare.Context context) {

    CreateTableExecutor exec = new CreateTableExecutor(create, context);

    /* Create an empty table with the given name
     * and the given columns.
     * That ensures, that the validator can validate (semantic)
     * sql queries against this table.
     */

    try {
      exec.execute();

      TableColumn linkerColumn = new TableColumn(
          config.getLinkerName(),
          SqlNumericLiteral.createCharString(
              config.getDefaultValue(),
              SqlParserPos.ZERO),
          new BasicSqlType(
              RelDataTypeSystemImpl.DEFAULT,
              config.getType(),
              config.getSize()),
          config.getColumnStrategy()
      );

      for (TableColumn column : exec.columns) {
        exec.executeWith(
            ScrambledbExecutor.config.createSubtableString(
                exec.getName(),
                column.getName()),
            ImmutableList.<TableColumn>builder()
                .add(linkerColumn)
                .add(column)
                .build());
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /** Executes a {@code DROP TABLE} command. */
  public static void execute(SqlDropObject drop,
      CalcitePrepare.Context context) {

    DropTableExecutor exec = new DropTableExecutor(drop, context);

    List<String> rootTableColumnNames = exec.getRootTableColumnsNames();

    for (String name : rootTableColumnNames) {
      exec.executeWith(ScrambledbExecutor.config.createSubtableString(
          exec.getName(),
          name));
    }

    /* Delete the empty table.
     */
    exec.execute();

  }

}
