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

package org.apache.calcite.scrambledb.converterConnection;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.scrambledb.ScrambledbExecutor;
import org.apache.calcite.scrambledb.converterConnection.kafka.KafkaConverterConnection;
import org.apache.calcite.scrambledb.converterConnection.rest.RestConverterConnection;

public class ConverterConnectionFactory {

  private ConverterConnectionFactory() {}

  public static ConverterConnection getConverterConnection(CalcitePrepare.Context context) {
    ConverterConnection.Type type = ScrambledbExecutor.config.getConnectionType();

    switch (type) {
    case REST:
      return new RestConverterConnection(context);
    case KAFKA:
      return new KafkaConverterConnection(context);
    default:
      System.out.println("[Error] could not find connection type for converter. Switched to default: REST");
      return new RestConverterConnection(context);
    }
  }

}
