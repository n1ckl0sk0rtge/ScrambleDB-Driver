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

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.scrambledb.rest.model.ConversionRequest;
import org.apache.calcite.scrambledb.rest.model.GenerationRequest;

import org.jboss.resteasy.client.jaxrs.ResteasyWebTarget;
import org.jboss.resteasy.client.jaxrs.internal.ResteasyClientBuilderImpl;

import java.util.List;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

/**
 * Wrapper class for relevant rest functionality for scrambleDB.
 */
public class ScrambledbRestClient {

  private final RestServicesInterface proxy;
  private final Client client;
  private final String apikey;

  public ScrambledbRestClient(CalcitePrepare.Context context) {
    this.apikey = context.config().converterAPIKey();
    this.client = ResteasyClientBuilderImpl.newClient();
    WebTarget target = client.target(context.config().converterConnection());
    ResteasyWebTarget restTarget = (ResteasyWebTarget) target;
    this.proxy = restTarget.proxy(RestServicesInterface.class);
  }

  public List<String> getPseudonyms(List<String> input) {
    GenerationRequest request = new GenerationRequest(this.apikey, input);
    Response response = proxy.pseudonyms(request);
    List<String> res = response.readEntity(List.class);
    response.close();
    return res;
  }

  public List<String> convert(List<String> pseudonyms) {
    ConversionRequest request = new ConversionRequest(this.apikey, pseudonyms);
    Response response = proxy.convert(request);
    List<String> res = response.readEntity(List.class);
    response.close();
    return res;
  }

  public void close() {
    this.client.close();
  }

}
