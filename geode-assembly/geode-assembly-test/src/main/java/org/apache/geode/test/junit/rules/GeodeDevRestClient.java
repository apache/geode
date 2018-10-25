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

package org.apache.geode.test.junit.rules;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

/**
 * this doesn't need to be a rule, since there is no setup and cleanup to do, just a utility
 * to issue rest call. It's different from GeodeHttpClientRule in that it creates a httpClient
 * in every rest call, while GeodeHttpClientRule uses one httpClient for the duration of the
 * entire test.
 */
public class GeodeDevRestClient {
  public static final String CONTEXT = "/geode/v1";

  private String bindAddress;
  private int restPort;
  private boolean useSsl;
  private HttpHost host;

  public GeodeDevRestClient(int restPort) {
    this("localhost", restPort, false);
  }

  public GeodeDevRestClient(String bindAddress, int restPort) {
    this(bindAddress, restPort, false);
  }

  public GeodeDevRestClient(String bindAddress, int restPort, boolean useSsl) {
    this.bindAddress = bindAddress;
    this.restPort = restPort;
    this.useSsl = useSsl;
    host = new HttpHost(bindAddress, restPort, useSsl ? "https" : "http");
  }

  public HttpResponse doHEAD(String query, String username, String password) {
    HttpHead httpHead = new HttpHead(CONTEXT + query);
    return doRequest(httpHead, username, password);
  }

  public HttpResponse doPost(String query, String username, String password, String body) {
    HttpPost httpPost = new HttpPost(CONTEXT + query);
    httpPost.addHeader("content-type", "application/json");
    httpPost.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(httpPost, username, password);
  }

  public HttpResponse doPut(String query, String username, String password, String body) {
    HttpPut httpPut = new HttpPut(CONTEXT + query);
    httpPut.addHeader("content-type", "application/json");
    httpPut.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(httpPut, username, password);
  }

  public HttpResponse doGet(String uri, String username, String password) {
    HttpGet getRequest = new HttpGet(CONTEXT + uri);
    return doRequest(getRequest, username, password);
  }

  public HttpResponse doDelete(String uri, String username, String password) {
    HttpDelete httpDelete = new HttpDelete(CONTEXT + uri);
    return doRequest(httpDelete, username, password);
  }

  public HttpResponseAssert doGetAndAssert(String uri) {
    return new HttpResponseAssert("Get " + uri, doGet(uri, null, null));
  }

  public HttpResponseAssert doGetAndAssert(String uri, String username, String password) {
    return new HttpResponseAssert("Get " + uri, doGet(uri, username, password));
  }


  public HttpResponseAssert doPutAndAssert(String uri, String body) {
    return new HttpResponseAssert("Put " + uri, doPut(uri, null, null, body));
  }

  public HttpResponseAssert doPostAndAssert(String uri, String body) {
    return new HttpResponseAssert("Post " + uri, doPost(uri, null, null, body));
  }

  public HttpResponseAssert doDeleteAndAssert(String uri) {
    return new HttpResponseAssert("Delete " + uri, doDelete(uri, null, null));
  }

  /**
   * this handles rest calls. each request creates a different httpClient object
   */
  public HttpResponse doRequest(HttpRequestBase request, String username, String password) {
    HttpClientBuilder clientBuilder = HttpClients.custom();
    HttpClientContext clientContext = HttpClientContext.create();

    // configures the clientBuilder and clientContext
    if (username != null) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(new AuthScope(bindAddress, restPort),
          new UsernamePasswordCredentials(username, password));
      clientBuilder.setDefaultCredentialsProvider(credsProvider);
    }

    try {
      if (useSsl) {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(new KeyManager[0], new TrustManager[] {new DefaultTrustManager()},
            new SecureRandom());
        clientBuilder.setSSLContext(ctx);
        clientBuilder.setSSLHostnameVerifier(new NoopHostnameVerifier());
      }

      return clientBuilder.build().execute(host, request, clientContext);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public static class DefaultTrustManager implements X509TrustManager {

    @Override
    public void checkClientTrusted(X509Certificate[] arg0, String arg1)
        throws CertificateException {}

    @Override
    public void checkServerTrusted(X509Certificate[] arg0, String arg1)
        throws CertificateException {}

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return null;
    }
  }
}
