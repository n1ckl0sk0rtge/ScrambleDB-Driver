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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.scrambledb.converterConnection.ConverterConnection;
import org.apache.calcite.scrambledb.converterConnection.kafka.KafkaConverterConnection;
import org.apache.calcite.scrambledb.converterConnection.rest.RestConverterConnection;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.UUID;

/**
 * Config for scrambleDB.
 */
public class ScrambledbConfig {

  public static final ScrambledbConfig INSTANCE = new ScrambledbConfig();

  private final String linkerName = "linkerID";
  private static final String defaultValue = "Null";
  private final Integer size = 500;
  private final SqlTypeName type = SqlTypeName.VARCHAR;
  private static final ColumnStrategy columnStrategy = ColumnStrategy.DEFAULT;
  private static final String subTableConnector = "_";
  private static final ConverterConnection.Type connectionType =
      ConverterConnection.Type.KAFKA;

  private final UUID kafka_identifier = UUID.randomUUID();
  private final RelDataType linkerRelDataType;

  private ScrambledbConfig() {
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    RelDataTypeFactory.Builder relDataBuilder =
        new RelDataTypeFactory.Builder(typeFactory);
    relDataBuilder
        .add(linkerName, type, size).nullable(true);

    this.linkerRelDataType = relDataBuilder.build();
  }

  public ConverterConnection getConverterConnection(CalcitePrepare.Context context) {
    switch (this.getConnectionType()) {
    case REST:
      return new RestConverterConnection(context);
    case KAFKA:
      return new KafkaConverterConnection(context, this.kafka_identifier);
    default:
      System.out.println("[Error] could not find connection type for converter. Switched to default: REST");
      return new RestConverterConnection(context);
    }
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
