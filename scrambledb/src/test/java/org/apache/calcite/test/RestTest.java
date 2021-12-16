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

import org.apache.calcite.scrambledb.rest.ScrambledbRestClient;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Class to test rest client.
 */
public class RestTest {

  @Test public void run() {
    ScrambledbRestClient client = new ScrambledbRestClient();
    List<String> inp = Arrays.asList("primaryKey1", "primaryKey1");
    // generate Pseudonyms from primary keys
    List<String> res = client.getPseudonyms(inp);
    // convert those pseudonyms to check if they have the same identity
    List<String> res_2 = client.convert(res);
    assertThat(res_2.get(0), is(res_2.get(1)));
  }

}
