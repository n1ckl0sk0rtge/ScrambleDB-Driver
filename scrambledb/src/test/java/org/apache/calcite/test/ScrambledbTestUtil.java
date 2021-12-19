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
package org.apache.calcite.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Provides utils fpr various test cases.
 *
 */
public class ScrambledbTestUtil {

  static String config =
      "jdbc:calcite:schemaFactory=org.apache.calcite.adapter.jdbc.JdbcSchema$Factory;"
          + "parserFactory=org.apache.calcite.scrambledb.ScrambledbExecutor#PARSER_FACTORY;"
          + "rewriterFactory=org.apache.calcite.scrambledb.rewriter.ScrambledbRewriterFactory#FACTORY;"
          + "converter.url=http://192.168.64.3:8080;"
          + "converter.apikey=test;"
          + "schema.jdbcDriver=com.mysql.cj.jdbc.Driver;"
          + "schema.jdbcUrl=jdbc:mysql://192.168.64.3/datalake;"
          + "schema.jdbcUser=datalake;"
          + "schema.jdbcPassword=datalake;";

  private ScrambledbTestUtil() {    }

  public static Connection getConnection() throws ClassNotFoundException, SQLException {
    Class.forName("org.apache.calcite.jdbc.Driver");
    return DriverManager.getConnection(config);
  }

  public static String resultToString(ResultSet resultSet) throws SQLException {
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      for (int i = 1; i <= n; i++) {
        buf.append(i > 1 ? "; " : "")
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=");
        if (resultSet.getObject(i) != null) {
          buf.append(resultSet.getObject(i).toString());
        } else {
          buf.append("null");
        }
      }
      buf.append("\n");
    }
    return buf.toString();
  }

}
