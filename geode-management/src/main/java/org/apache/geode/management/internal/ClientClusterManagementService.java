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


import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriTemplateHandler;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.RestfulEndpoint;

/**
 * Implementation of {@link ClusterManagementService} interface which represents the cluster
 * management service as used by a Java client.
 * <p/>
 * In order to manipulate Geode components (Regions, etc.) clients can construct instances of {@link
 * CacheElement}s and call the corresponding
 * {@link ClientClusterManagementService#create(CacheElement,
 * String)}, {@link ClientClusterManagementService#delete(CacheElement, String)} or {@link
 * ClientClusterManagementService#update(CacheElement, String)} method. The returned {@link
 * ClusterManagementResult} will contain all necessary information about the outcome of the call.
 * This will include the result of persisting the config as part of the cluster configuration as
 * well as creating the actual component in the cluster.
 * <p/>
 * All create calls are idempotent and will not return an error if the desired component already
 * exists.
 */
public class ClientClusterManagementService implements ClusterManagementService {

  private static final ResponseErrorHandler DEFAULT_ERROR_HANDLER =
      new RestTemplateResponseErrorHandler();

  private static final String VERSION = "/v2";

  private final RestTemplate restTemplate;

  private ClientClusterManagementService() {
    restTemplate = new RestTemplate();
    restTemplate.setErrorHandler(DEFAULT_ERROR_HANDLER);
  }

  public ClientClusterManagementService(String host, int port) {
    this(host, port, null, null, null, null);
  }

  public ClientClusterManagementService(String host, int port, SSLContext sslContext,
      HostnameVerifier hostnameVerifier, String username, String password) {
    this();

    DefaultUriTemplateHandler templateHandler = new DefaultUriTemplateHandler();
    String schema = (sslContext == null) ? "http" : "https";
    templateHandler.setBaseUrl(schema + "://" + host + ":" + port + "/geode-management");
    restTemplate.setUriTemplateHandler(templateHandler);

    // HttpComponentsClientHttpRequestFactory allows use to preconfigure httpClient for
    // authentication and ssl context
    HttpComponentsClientHttpRequestFactory requestFactory =
        new HttpComponentsClientHttpRequestFactory();

    HttpClientBuilder clientBuilder = HttpClientBuilder.create();
    // configures the clientBuilder
    if (username != null) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(new AuthScope(host, port),
          new UsernamePasswordCredentials(username, password));
      clientBuilder.setDefaultCredentialsProvider(credsProvider);
    }

    clientBuilder.setSSLContext(sslContext);
    clientBuilder.setSSLHostnameVerifier(hostnameVerifier);

    requestFactory.setHttpClient(clientBuilder.build());
    restTemplate.setRequestFactory(requestFactory);
  }

  public ClientClusterManagementService(ClientHttpRequestFactory requestFactory) {
    this();
    this.restTemplate.setRequestFactory(requestFactory);
  }

  @Override
  public ClusterManagementResult create(CacheElement config) {
    String endPoint = getEndpoint(config);
    // the response status code info is represented by the ClusterManagementResult.errorCode already
    return restTemplate
        .postForEntity(VERSION + endPoint, config, ClusterManagementResult.class)
        .getBody();
  }

  @Override
  public ClusterManagementResult delete(CacheElement config) {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  public ClusterManagementResult update(CacheElement config) {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  public ClusterManagementResult list(CacheElement config) {
    String endPoint = getEndpoint(config);
    String id = config.getId();

    // return restTemplate
    // .getForEntity(VERSION + endPoint + ((id == null) ? "" : "/{id}"),
    // ClusterManagementResult.class, id)
    // .getBody();

    return restTemplate
        .getForEntity(VERSION + endPoint + ((id == null) ? "" : "/?id=" + id),
            ClusterManagementResult.class)
        .getBody();

  }

  public RestTemplate getRestTemplate() {
    return restTemplate;
  }

  private String getEndpoint(CacheElement config) {
    if (!(config instanceof RestfulEndpoint)) {
      throw new IllegalArgumentException(
          String.format("The config type %s does not have a RESTful endpoint defined",
              config.getClass().getName()));
    }

    return ((RestfulEndpoint) config).getEndpoint();
  }

  public boolean isConnected() {
    return restTemplate.getForEntity(VERSION + "/ping", String.class)
        .getBody().equals("pong");
  }

}
