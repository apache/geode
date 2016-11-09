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

package org.apache.geode.rest.internal.web;

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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class GeodeRestClient {
  public final static String CONTEXT = "/geode/v1";

  private int restPort = 0;
  private String bindAddress = null;
  private String protocol = "http";
  private boolean useHttps = false;
  private KeyStore keyStore;

  public GeodeRestClient(String bindAddress, int restPort) {
    this.bindAddress = bindAddress;
    this.restPort = restPort;
  }

  public GeodeRestClient(String bindAddress, int restPort, boolean useHttps) {
    if (useHttps) {
      this.protocol = "https";
      this.useHttps = true;
    } else {
      this.protocol = "http";
      this.useHttps = false;
    }
    this.bindAddress = bindAddress;
    this.restPort = restPort;
  }

  public static String getContentType(HttpResponse response) {
    return response.getEntity().getContentType().getValue();
  }

  /**
   * Retrieve the status code of the HttpResponse
   * 
   * @param response The HttpResponse message received from the server
   * @return a numeric value
   */
  public static int getCode(HttpResponse response) {
    return response.getStatusLine().getStatusCode();
  }

  public static JSONObject getJsonObject(HttpResponse response) throws IOException, JSONException {
    JSONTokener tokener = new JSONTokener(new InputStreamReader(response.getEntity().getContent()));
    return new JSONObject(tokener);
  }

  public static JSONArray getJsonArray(HttpResponse response) throws IOException, JSONException {
    JSONTokener tokener = new JSONTokener(new InputStreamReader(response.getEntity().getContent()));
    return new JSONArray(tokener);
  }

  public HttpResponse doHEAD(String query, String username, String password) throws Exception {
    HttpHead httpHead = new HttpHead(CONTEXT + query);
    return doRequest(httpHead, username, password);
  }

  public HttpResponse doPost(String query, String username, String password, String body)
      throws Exception {
    HttpPost httpPost = new HttpPost(CONTEXT + query);
    httpPost.addHeader("content-type", "application/json");
    httpPost.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(httpPost, username, password);
  }

  public HttpResponse doPut(String query, String username, String password, String body)
      throws Exception {
    HttpPut httpPut = new HttpPut(CONTEXT + query);
    httpPut.addHeader("content-type", "application/json");
    httpPut.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
    return doRequest(httpPut, username, password);
  }

  public HttpResponse doGet(String uri, String username, String password) throws Exception {
    HttpGet getRequest = new HttpGet(CONTEXT + uri);
    return doRequest(getRequest, username, password);
  }

  public HttpResponse doGetRequest(String url) throws Exception {
    HttpGet getRequest = new HttpGet(url);
    return doRequest(getRequest, null, null);
  }

  public HttpResponse doDelete(String uri, String username, String password) throws Exception {
    HttpDelete httpDelete = new HttpDelete(CONTEXT + uri);
    return doRequest(httpDelete, username, password);
  }

  public HttpResponse doRequest(HttpRequestBase request, String username, String password)
      throws Exception {
    HttpHost targetHost = new HttpHost(bindAddress, restPort, protocol);

    HttpClientBuilder clientBuilder = HttpClients.custom();
    HttpClientContext clientContext = HttpClientContext.create();

    // configures the clientBuilder and clientContext
    if (username != null) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()),
          new UsernamePasswordCredentials(username, password));
      clientBuilder.setDefaultCredentialsProvider(credsProvider);
    }

    if (useHttps) {
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(new KeyManager[0], new TrustManager[] {new DefaultTrustManager()},
          new SecureRandom());
      clientBuilder.setSSLContext(ctx);
      clientBuilder.setSSLHostnameVerifier(new NoopHostnameVerifier());
    }

    return clientBuilder.build().execute(targetHost, request, clientContext);
  }

  private static class DefaultTrustManager implements X509TrustManager {

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
