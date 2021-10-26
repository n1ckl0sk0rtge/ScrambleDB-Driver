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

import org.apache.calcite.scrambledb.ScrambledbUtil;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

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

    boolean b = statement.execute("create table customer (name varchar(20), age int default 0)");
    assertThat(b, is(false));
    int y = statement.executeUpdate("insert into customer (name) values ('max')");
    assertThat(y, is(1));
    int x = statement.executeUpdate("insert into customer (name, age) values ('lisa', 31)");
    assertThat(x, is(1));
    x = statement.executeUpdate("insert into customer values ('lucas', 12)");
    assertThat(x, is(1));
    //statement.execute("select * from t");
    /*try (ResultSet r = statement.executeQuery("select sum(i) from t")) {
      assertThat(r.next(), is(true));
      assertThat(r.getInt(1), is(4));
      assertThat(r.next(), is(false));
    }*/
    statement.execute("drop table customer");

    statement.close();
    connection.close();
  }


}
