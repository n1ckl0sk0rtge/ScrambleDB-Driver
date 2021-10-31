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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.ContextSqlValidator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

/** A class for utility function related to Scrmbledb.
 */
public class ScrambledbUtil {

  /** Returns the schema in which to create an object. */
  public static Pair<CalciteSchema, String> schema(CalcitePrepare.Context context,
      boolean mutable, SqlIdentifier id) {
    final String name;
    final List<String> path;
    if (id.isSimple()) {
      path = context.getDefaultSchemaPath();
      name = id.getSimple();
    } else {
      path = Util.skipLast(id.names);
      name = Util.last(id.names);
    }
    CalciteSchema schema = mutable ? context.getMutableRootSchema()
        : context.getRootSchema();
    for (String p : path) {
      schema = schema.getSubSchema(p, true);
    }
    return Pair.of(schema, name);
  }

  public  static CalciteSchema schema(CalcitePrepare.Context context, boolean mutable) {
    final List<String> path = context.getDefaultSchemaPath();

    CalciteSchema schema = mutable ? context.getMutableRootSchema()
        : context.getRootSchema();
    for (String p : path) {
      assert schema != null;
      schema = schema.getSubSchema(p, true);
    }
    return schema;
  }


  public static @Nullable <T> RelNode contains(RelNode node, Class<T> type) {
    if (node.getClass() == type) {
      return node;
    }
    for (int i = 0; i < node.getInputs().size(); i++) {
      return contains(node.getInput(i), type);
    }
    return null;
  }

  public static <T> List<RelNode> containsMultiple(RelNode node, Class<T> type) {
    return containsMultiple(new ArrayList<>(), node, type);
  }

  private static <T> List<RelNode> containsMultiple(List<RelNode> nodes, RelNode node, Class<T> type) {
    if (node.getClass() == type) {
      nodes.add(node);
    }
    for (int i = 0; i < node.getInputs().size(); i++) {
      containsMultiple(nodes, node.getInput(i), type);
    }
    return nodes;
  }

  /**
   * Returns the SqlValidator with the given {@code context} schema
   * and type factory.
   */
  public static SqlValidator validator(CalcitePrepare.Context context,
      boolean mutable) {
    return new ContextSqlValidator(context, mutable);
  }

}
