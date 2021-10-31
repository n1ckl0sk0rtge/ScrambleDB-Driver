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

package org.apache.calcite.scrambledb.rewriter;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.scrambledb.rewriter.rules.ScrambledbInsertRule;
import org.apache.calcite.scrambledb.rewriter.rules.ScrambledbSelectRule;
import org.apache.calcite.tools.SqlRewriterImpl;
import org.apache.calcite.tools.SqlRewriterRule;

import java.util.ArrayList;
import java.util.List;

public class ScrambledbRewriter implements SqlRewriterImpl {

  private final List<SqlRewriterRule> rules = new ArrayList<>();

  ScrambledbRewriter() {
    rules.add(new ScrambledbInsertRule());
    rules.add(new ScrambledbSelectRule());
  }

  @Override
  public RelRoot rewrite(RelRoot root, CalcitePrepare.Context context) {

    for (SqlRewriterRule rule : rules) {
      if (rule.isApplicable(root.rel, root.kind)) {
        try {
          root = RelRoot.of(
              rule.apply(root.rel, context),
              root.kind);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    return root;
  }

}
