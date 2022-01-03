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
package org.apache.calcite.scrambledb.converterConnection.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;

import java.util.List;

/**
 * Pattern for generation request.
 */
@JsonSchemaInject(strings = {@JsonSchemaString(path="javaType",
    value="org.apache.calcite.scrambledb.converterConnection.model.Payload")})
public class Payload {

  @JsonProperty
  private List<String> data;

  Payload() {}

  public Payload(List<String> data) {
    this.data = data;
  }

  public List<String> getData() {
    return data;
  }
}
