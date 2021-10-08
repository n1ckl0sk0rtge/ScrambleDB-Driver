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
package org.apache.calcite.scrambledb.parser;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlNode;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * Class to define Columns in a table.
 */
public class TableColumn {
  final SqlNode expr;
  final RelDataType type;
  final ColumnStrategy strategy;

  public TableColumn(SqlNode expr, RelDataType type,
      ColumnStrategy strategy) {
    this.expr = expr;
    this.type = type;
    this.strategy = Objects.requireNonNull(strategy, "strategy");
    Preconditions.checkArgument(
        strategy == ColumnStrategy.NULLABLE
            || strategy == ColumnStrategy.NOT_NULLABLE
            || expr != null);
  }

  public SqlNode getExpr() {
    return expr;
  }

  public RelDataType getType() {
    return type;
  }

  public ColumnStrategy getStrategy() {
    return strategy;
  }

  /**
   * Return TableColumn of a given table.
   */
  public static TableColumn of(SqlNode expr, RelDataType type,
      ColumnStrategy strategy) {
    return new TableColumn(expr, type, strategy);
  }

}
