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

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;

import java.util.Stack;

/**
 * Specific error definitions for ScrambleDB.
 */
public class ScrambledbErrors {
  /**
   * Error definition for insert query.
   */
  public static class RewriteInsertRuleError extends Exception {
    public RewriteInsertRuleError(RelNode node) {
      super("Error while rewriting insert statement: n"
          + node.explain());
    }
  }

  /**
   * Error definition for create table.
   */
  public static class CreateTableFunctionalityIsNotPartOfSchema extends Exception {
    public CreateTableFunctionalityIsNotPartOfSchema(CalciteSchema schema) {
      super("Create table functionality is not part of schema: "
          + schema.schema);
    }
  }

  /**
   * Error definition for select table query.
   */
  public static class SelectReplacementError extends Exception {
    public SelectReplacementError(RelNode currentNode, Stack<RelNode> replaceNodes) {
      super(
          "Error while replacing select statements (JdbcTableScan) with custom ScrambledbScans"
              + " by node"
              + currentNode.explain()
              + ". ReplacementNodes: "
              + replaceNodes.toString());
    }
  }

}
