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
package org.apache.calcite.scrambledb.rest;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.tools.ConverterConnection;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.sun.org.apache.xerces.internal.impl.io.UTF8Reader.DEFAULT_BUFFER_SIZE;

/**
 * Wrapper class for relevant rest functionality for scrambleDB.
 */
public class RestConverterConnection implements ConverterConnection {

  private final CalcitePrepare.Context context;

  public RestConverterConnection(CalcitePrepare.Context context) {
    // get api key from config
    this.context = context;
  }

  @Override public Type getType() {
    return Type.REST;
  }

  @Override public List<String> getPseudonyms(List<String> input) {
    return request(input, "/api/pseudonym");
  }

  @Override public List<String> convert(List<String> pseudonyms) {
    return request(pseudonyms, "/api/convert");
  }

  private List<String> request(List<String> input, String endpoint) {
    // get connection information from config
    try {
      URL url = new URL(this.context.config().converterRestServer() + endpoint);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestProperty("Content-Type", "application/json;");
      connection.setRequestMethod("POST");
      connection.setDoOutput(true);

      try (OutputStream os = connection.getOutputStream()) {
        byte[] payload = getJsonString(input).getBytes(StandardCharsets.UTF_8);
        os.write(payload, 0, payload.length);
      }

      InputStream inputStream = connection.getInputStream();
      String response = convertInputStreamToString(inputStream);
      // remove brackets
      StringBuilder sb = new StringBuilder(response);
      sb.deleteCharAt(response.length() - 1);
      sb.deleteCharAt(0);
      String[] str_response = sb.toString().split(",");
      List<String> output = new ArrayList<>();
      for (String str : str_response) {
        sb = new StringBuilder(str);
        sb.deleteCharAt(str.length() - 1);
        sb.deleteCharAt(0);
        output.add(sb.toString());
      }
      return output;

    } catch (Exception e) {
      e.printStackTrace();
      return new ArrayList<>();
    }

  }

  private String getJsonString(List<String> data) {
    StringBuilder sb = new StringBuilder();
    sb.append("{").append("\"data\"").append(": ").append("[");
    int counter = 0;
    for (String e : data) {
      sb.append("\"").append(e).append("\"");
      if (counter != data.size() -1) {
        sb.append(",");
      }
      counter ++;
    }
    sb.append("]").append(" ").append("}");
    return sb.toString();
  }

  private static String convertInputStreamToString(InputStream is) throws IOException {

    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
    int length;
    while ((length = is.read(buffer)) != -1) {
      result.write(buffer, 0, length);
    }

    return result.toString(StandardCharsets.UTF_8.name());
  }

}
