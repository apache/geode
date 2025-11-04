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

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.StringEntity;

/**
 * this doesn't need to be a rule, since there is no setup and cleanup to do, just a utility
 * to issue rest call. It's different from GeodeHttpClientRule in that it creates a httpClient
 * in every rest call, while GeodeHttpClientRule uses one httpClient for the duration of the
 * entire test.
 *
 * <p>
 * <b>Jakarta EE 10 Migration Changes:</b>
 * <ul>
 * <li>Apache HttpComponents 4.x → 5.x (required for Jakarta compatibility)</li>
 * <li>Package changes: org.apache.http.* → org.apache.hc.client5.* and org.apache.hc.core5.*</li>
 * <li>Return type: HttpResponse → ClassicHttpResponse</li>
 * <li>Request base class: HttpRequestBase → HttpUriRequestBase</li>
 * <li>HttpHost constructor: HttpHost(host, port, scheme) → HttpHost(scheme, host, port)</li>
 * <li>UsernamePasswordCredentials: String password → char[] password</li>
 * <li>SSL config: Direct setSSLContext() → Connection manager with SSL socket factory</li>
 * <li>NoopHostnameVerifier: new instance → static INSTANCE field</li>
 * </ul>
 */
public class GeodeDevRestClient {
  public static final String CONTEXT = "/geode/v1";

  private final String context;
  private final String bindAddress;
  private final int restPort;
  private final boolean useSsl;
  private final HttpHost host;


  public GeodeDevRestClient(String bindAddress, int restPort) {
    this(CONTEXT, bindAddress, restPort, false);
  }

  public GeodeDevRestClient(String bindAddress, int restPort, boolean useSsl) {
    this(CONTEXT, bindAddress, restPort, useSsl);
  }

  public GeodeDevRestClient(String context, String bindAddress, int restPort, boolean useSsl) {
    this.context = context;
    this.bindAddress = bindAddress;
    this.restPort = restPort;
    this.useSsl = useSsl;
    // HttpClient 5.x: HttpHost constructor parameter order changed to (scheme, host, port)
    host = new HttpHost(useSsl ? "https" : "http", bindAddress, restPort);
  }

  public ClassicHttpResponse doHEAD(String query, String username, String password) {
    HttpHead httpHead = new HttpHead(context + query);
    return doRequest(httpHead, username, password);
  }

  public ClassicHttpResponse doPost(String query, String username, String password, String body) {
    HttpPost httpPost = new HttpPost(context + query);
    httpPost.addHeader("content-type", "application/json");
    httpPost.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(httpPost, username, password);
  }

  public ClassicHttpResponse doPut(String query, String username, String password, String body) {
    HttpPut httpPut = new HttpPut(context + query);
    httpPut.addHeader("content-type", "application/json");
    httpPut.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(httpPut, username, password);
  }

  public ClassicHttpResponse doGet(String uri, String username, String password) {
    HttpGet getRequest = new HttpGet(context + uri);
    return doRequest(getRequest, username, password);
  }

  public ClassicHttpResponse doDelete(String uri, String username, String password) {
    HttpDelete httpDelete = new HttpDelete(context + uri);
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

  public HttpResponseAssert doPostAndAssert(String uri, String body, String username,
      String password) {
    return new HttpResponseAssert("Post " + uri, doPost(uri, username, password, body));
  }

  public HttpResponseAssert doDeleteAndAssert(String uri) {
    return new HttpResponseAssert("Delete " + uri, doDelete(uri, null, null));
  }

  /*
   * this handles rest calls. each request creates a different httpClient object
   */
  public ClassicHttpResponse doRequest(HttpUriRequestBase request, String username,
      String password) {
    HttpClientBuilder clientBuilder = HttpClients.custom();
    HttpClientContext clientContext = HttpClientContext.create();

    // configures the clientBuilder and clientContext
    if (username != null) {
      BasicCredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(new AuthScope(bindAddress, restPort),
          // HttpClient 5.x: UsernamePasswordCredentials now takes char[] for password (security)
          new UsernamePasswordCredentials(username,
              password != null ? password.toCharArray() : null));
      clientBuilder.setDefaultCredentialsProvider(credsProvider);
    }

    try {
      if (useSsl) {
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(new KeyManager[0], new TrustManager[] {new DefaultTrustManager()},
            new SecureRandom());
        // HttpClient 5.x: SSL configuration changed from direct setSSLContext() to
        // connection manager with SSL socket factory. This provides more granular SSL control.
        // NoopHostnameVerifier changed from new instance to static INSTANCE field.
        HttpClientConnectionManager connectionManager =
            PoolingHttpClientConnectionManagerBuilder.create()
                .setSSLSocketFactory(SSLConnectionSocketFactoryBuilder.create()
                    .setSslContext(ctx)
                    .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .build())
                .build();
        clientBuilder.setConnectionManager(connectionManager);
      }

      return clientBuilder.build().execute(host, request, clientContext);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Request %s' with username '%s' password '%s' failed", request, username,
              password),
          e);
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
