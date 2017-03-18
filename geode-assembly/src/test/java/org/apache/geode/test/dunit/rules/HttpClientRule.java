/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.test.dunit.rules;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.junit.Before;
import org.junit.rules.ExternalResource;

import java.util.ArrayList;
import java.util.List;

public class HttpClientRule extends ExternalResource {
  private String hostName;
  private int port;
  private HttpHost host;
  private HttpClient httpClient;
  private HttpContext context;

  public HttpClientRule(int port) {
    this("localhost", port);
  }

  public HttpClientRule(String hostName, int port) {
    this.hostName = hostName;
    this.port = port;
  }

  @Before
  protected void before() {
    host = new HttpHost(hostName, port);
    httpClient = HttpClients.createDefault();
    context = new BasicHttpContext();
  }

  public HttpResponse loginToPulse(String username, String password) throws Exception {
    return post("/pulse/login", "username", username, "password", password);
  }

  public void loginToPulseAndVerify(String username, String password) throws Exception {
    HttpResponse response = loginToPulse(username, password);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/clusterDetail.html");
  }


  public HttpResponse get(String uri, String... params) throws Exception {
    return httpClient.execute(host, buildHttpGet(uri, params));
  }

  public HttpResponse post(String uri, String... params) throws Exception {
    return httpClient.execute(host, buildHttpPost(uri, params));
  }

  private HttpPost buildHttpPost(String uri, String... params) throws Exception {
    HttpPost post = new HttpPost(uri);
    List<NameValuePair> nvps = new ArrayList<>();
    for (int i = 0; i < params.length; i += 2) {
      nvps.add(new BasicNameValuePair(params[i], params[i + 1]));
    }
    post.setEntity(new UrlEncodedFormEntity(nvps));
    return post;
  }

  private HttpGet buildHttpGet(String uri, String... params) throws Exception {
    URIBuilder builder = new URIBuilder();
    builder.setPath(uri);
    for (int i = 0; i < params.length; i += 2) {
      builder.setParameter(params[i], params[i + 1]);
    }
    return new HttpGet(builder.build());
  }



}
