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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.scrambledb.converterConnection.ConverterConnection;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
/**
 * Config for scrambleDB.
 */
public class ScrambledbConfig {

  private final String linkerName = "linkerID";
  private final String defaultValue = "Null";
  private final Integer size = 500;
  private final SqlTypeName type = SqlTypeName.VARCHAR;
  private final ColumnStrategy columnStrategy = ColumnStrategy.DEFAULT;
  private final String subTableConnector = "_";
  private final ConverterConnection.Type connectionType = ConverterConnection.Type.REST;

  private final RelDataType linkerRelDataType;

  ScrambledbConfig() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    RelDataTypeFactory.Builder relDataBuilder =
        new RelDataTypeFactory.Builder(typeFactory);
    relDataBuilder
        .add(linkerName, this.type, this.size).nullable(true);

    this.linkerRelDataType = relDataBuilder.build();
  }

  public String createSubTableString(String rootTableName, String columName) {
    return rootTableName + subTableConnector + columName;
  }

  public ConverterConnection.Type getConnectionType() {
    return connectionType;
  }

  public Integer getSize() {
    return size;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public SqlTypeName getType() {
    return type;
  }

  public ColumnStrategy getColumnStrategy() {
    return columnStrategy;
  }

  public String getLinkerName() {
    return linkerName;
  }

  public RelDataType getLinkerRelDataType() {
    return linkerRelDataType;
  }

  public List<RelDataTypeField> getLinkerRelDataTypeField() {
    return linkerRelDataType.getFieldList();
  }
}
