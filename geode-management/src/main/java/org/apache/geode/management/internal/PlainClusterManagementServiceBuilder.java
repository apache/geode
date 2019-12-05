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

package org.apache.geode.management.internal;

import java.util.Arrays;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.Header;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriTemplateHandler;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;

public class PlainClusterManagementServiceBuilder implements
    ClusterManagementServiceBuilder.PlainBuilder {
  protected String host;
  protected int port;
  protected SSLContext sslContext;
  protected HostnameVerifier hostnameVerifier;
  protected String username;
  protected String password;
  protected String authToken;

  public PlainClusterManagementServiceBuilder() {}

  static final ResponseErrorHandler DEFAULT_ERROR_HANDLER =
      new RestTemplateResponseErrorHandler();

  public PlainClusterManagementServiceBuilder setHostAddress(String host, int port) {
    this.host = host;
    this.port = port;
    return this;
  }

  public PlainClusterManagementServiceBuilder setSslContext(SSLContext sslContext) {
    this.sslContext = sslContext;
    return this;
  }

  public PlainClusterManagementServiceBuilder setHostnameVerifier(
      HostnameVerifier hostnameVerifier) {
    this.hostnameVerifier = hostnameVerifier;
    return this;
  }

  public PlainClusterManagementServiceBuilder setCredentials(String username,
      String password) {
    this.username = username;
    this.password = password;
    return this;
  }

  @Override
  public ClusterManagementServiceBuilder.PlainBuilder setAuthToken(String authToken) {
    this.authToken = authToken;
    return this;
  }

  public String getUsername() {
    return username;
  }

  public ClusterManagementService build() {
    RestTemplate restTemplate = new RestTemplate();
    restTemplate.setErrorHandler(DEFAULT_ERROR_HANDLER);

    if (host == null || port <= 0) {
      throw new IllegalArgumentException(
          "host and port needs to be specified in order to build the service.");
    }

    DefaultUriTemplateHandler templateHandler = new DefaultUriTemplateHandler();
    String schema = (sslContext == null) ? "http" : "https";
    templateHandler.setBaseUrl(schema + "://" + host + ":" + port + "/management");
    restTemplate.setUriTemplateHandler(templateHandler);

    // HttpComponentsClientHttpRequestFactory allows use to preconfigure httpClient for
    // authentication and ssl context
    HttpComponentsClientHttpRequestFactory requestFactory =
        new HttpComponentsClientHttpRequestFactory();

    HttpClientBuilder clientBuilder = HttpClientBuilder.create();
    // configures the clientBuilder
    if (authToken != null) {
      List<Header> defaultHeaders = Arrays.asList(
          new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + authToken));
      clientBuilder.setDefaultHeaders(defaultHeaders);
    } else if (username != null) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(new AuthScope(host, port),
          new UsernamePasswordCredentials(username, password));
      clientBuilder.setDefaultCredentialsProvider(credsProvider);
    }

    clientBuilder.setSSLContext(sslContext);
    clientBuilder.setSSLHostnameVerifier(hostnameVerifier);

    requestFactory.setHttpClient(clientBuilder.build());

    restTemplate.setRequestFactory(requestFactory);

    return new ClientClusterManagementService(restTemplate);
  }
}
