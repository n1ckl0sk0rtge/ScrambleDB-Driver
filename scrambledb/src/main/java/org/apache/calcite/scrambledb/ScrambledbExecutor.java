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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.ContextSqlValidator;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.CreateTable;
import org.apache.calcite.schema.DropTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.scrambledb.parser.SqlCreateTable;
import org.apache.calcite.scrambledb.parser.TableColumn;
import org.apache.calcite.scrambledb.tasks.CreateTableExecutor;
import org.apache.calcite.scrambledb.tasks.DropTableExecutor;
import org.apache.calcite.server.DdlExecutor;
import org.apache.calcite.server.DdlExecutorImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlDropObject;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.scrambledb.SqlScrambledbParserImpl;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.io.Reader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/** Executes ScrambleDB related commands.
 *
 * <p>Given a DDL command that is a sub-class of {@link org.apache.calcite.sql.SqlNode}, dispatches
 * the command to an appropriate {@code execute} method. For example,
 * "CREATE TABLE" ({@link SqlCreateTable}) is dispatched to
 * {@link #execute(SqlCreateTable, CalcitePrepare.Context)}. */
public class ScrambledbExecutor extends DdlExecutorImpl {

  public static final ScrambledbExecutor INSTANCE = new ScrambledbExecutor();

  protected ScrambledbExecutor() {}

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

  /** Executes a {@code CREATE SCRAMBLEDTABLE} command. */
  public void execute(SqlCreateTable create,
      CalcitePrepare.Context context) throws
      ScrambledbUtil.CreateTableFunctionalityIsNotPartOfSchema {

    CreateTableExecutor.execute(create, context);
  }

  /** Executes a {@code DROP TABLE} command. */
  public static void execute(SqlDropObject drop,
      CalcitePrepare.Context context) {

    DropTableExecutor.execute(drop, context);
  }

}
