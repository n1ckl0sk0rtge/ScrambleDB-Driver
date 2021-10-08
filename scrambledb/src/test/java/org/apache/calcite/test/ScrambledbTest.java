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

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Tests sql execution functionality.
 *
 */
public class ScrambledbTest {

  @Test public void testCreateScrambledTable() throws Exception {
    Connection connection = ScrambledbTestUtil.getConnection();
    Statement statement = connection.createStatement();

    boolean b = statement.execute("create scrambledtable t (i int not null)");
    assertThat(b, is(false));
    int x = statement.executeUpdate("insert into t values 1");
    assertThat(x, is(1));
    x = statement.executeUpdate("insert into t values 3");
    assertThat(x, is(1));
    try (ResultSet r = statement.executeQuery("select sum(i) from t")) {
      assertThat(r.next(), is(true));
      assertThat(r.getInt(1), is(4));
      assertThat(r.next(), is(false));
    }
    statement.execute("drop table t");

    statement.close();
    connection.close();
  }


}
