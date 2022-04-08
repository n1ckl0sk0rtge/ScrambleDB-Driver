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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.jdbc.JdbcTable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class DatabaseCall implements Runnable {

  public ResultSet resultSet = null;
  public Map<Object, Object[]> resultMap = null;

  private ThreadLocal<PreparedStatement> statementThreadLocal = null;

  private ThreadLocal<JdbcTable> tableThreadLocal = null;
  private ThreadLocal<DataContext> dataContextThreadLocal = null;

  private final boolean expectResult;

  private CountDownLatch countDownLatch;

  public DatabaseCall(
      PreparedStatement statement,
      boolean expectResult) {
    this.statementThreadLocal = ThreadLocal.withInitial(() -> statement);;
    this.expectResult = expectResult;
  }

  public DatabaseCall(
      JdbcTable table,
      DataContext dataContext,
      boolean expectResult,
      CountDownLatch countDownLatch
  ) {
    this.tableThreadLocal = ThreadLocal.withInitial(() -> table);
    this.dataContextThreadLocal = ThreadLocal.withInitial(() -> dataContext);
    this.expectResult = expectResult;
    this.countDownLatch = countDownLatch;
  }

  private void execute() throws SQLException {
    if (this.statementThreadLocal != null) {
      PreparedStatement p = this.statementThreadLocal.get();
      if (expectResult) {
        this.resultSet = p.executeQuery();
      } else {
        p.execute();
      }
      p.close();
    } else {
      JdbcTable table = this.tableThreadLocal.get();
      DataContext dataContext = this.dataContextThreadLocal.get();
      this.resultMap = table.scan(dataContext).toMap(key -> {
        // there should always be at least one element for each row in the result set.
        // only assert here to remove ide warning
        assert key[0] != null;
        return key[0];
      });
    }
  }

  @Override
  public void run() {
    try {
      execute();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    if (countDownLatch != null) {
      this.countDownLatch.countDown();
    }
  }

}
