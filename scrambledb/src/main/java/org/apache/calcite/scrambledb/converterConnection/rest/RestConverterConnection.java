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
package org.apache.calcite.scrambledb.converterConnection.rest;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.tools.ConverterConnection;
import org.apache.calcite.scrambledb.converterConnection.model.Payload;

import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl;

import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

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
    // create rest connection to converter
    Client client = ResteasyClientBuilderImpl.newClient();
    // get connection information from config
    WebTarget target = client.target(this.context.config().converterRestServer());
    ResteasyWebTarget restTarget = (ResteasyWebTarget) target;
    RestServicesInterface proxy = restTarget.proxy(RestServicesInterface.class);

    Payload payload = new Payload(input);
    Response response = proxy.pseudonyms(payload);

    List<String> res;
    try {
      res = response.readEntity(List.class);
    } catch (Exception e) {
      e.printStackTrace();
      res = new ArrayList<>();
    }

    response.close();
    client.close();
    return res;
  }

  @Override public List<String> convert(List<String> pseudonyms) {
    // create rest connection to converter
    Client client = ResteasyClientBuilderImpl.newClient();
    // get connection information from config
    WebTarget target = client.target(this.context.config().converterRestServer());
    ResteasyWebTarget restTarget = (ResteasyWebTarget) target;
    RestServicesInterface proxy = restTarget.proxy(RestServicesInterface.class);

    Payload payload = new Payload(pseudonyms);
    Response response = proxy.convert(payload);

    List<String> res;
    try {
      res = response.readEntity(List.class);
    } catch (Exception e) {
      e.printStackTrace();
      res = new ArrayList<>();
    }

    response.close();
    client.close();
    return res;
  }

}
