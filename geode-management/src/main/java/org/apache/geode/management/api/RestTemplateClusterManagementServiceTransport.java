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
 *
 */

package org.apache.geode.management.api;

import static org.apache.geode.management.configuration.Links.URI_VERSION;
import static org.apache.geode.management.internal.Constants.INCLUDE_CLASS_HEADER;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import org.apache.http.Header;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriTemplateHandler;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.configuration.AbstractConfiguration;
import org.apache.geode.management.internal.CompletableFutureProxy;
import org.apache.geode.management.internal.RestTemplateResponseErrorHandler;
import org.apache.geode.management.runtime.OperationResult;
import org.apache.geode.management.runtime.RuntimeInfo;
import org.apache.geode.util.internal.GeodeJsonMapper;

/**
 * Concrete implementation of {@link ClusterManagementServiceTransport} which uses Spring's
 * {@link RestTemplate} for communication between client and CMS endpoint.
 */
@Experimental
public class RestTemplateClusterManagementServiceTransport
    implements ClusterManagementServiceTransport {

  static final ResponseErrorHandler DEFAULT_ERROR_HANDLER =
      new RestTemplateResponseErrorHandler();

  private final RestTemplate restTemplate;

  private final ScheduledExecutorService longRunningStatusPollingThreadPool =
      Executors.newScheduledThreadPool(1);

  public RestTemplateClusterManagementServiceTransport(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
    this.restTemplate.setErrorHandler(DEFAULT_ERROR_HANDLER);
  }

  public RestTemplateClusterManagementServiceTransport(
      ConnectionConfig connectionConfig) {
    this(new RestTemplate(), connectionConfig);
  }

  public RestTemplateClusterManagementServiceTransport(RestTemplate restTemplate,
      ConnectionConfig connectionConfig) {
    this(restTemplate);
    configureConnection(connectionConfig);
  }

  public void configureConnection(ConnectionConfig connectionConfig) {
    if (connectionConfig == null) {
      throw new IllegalStateException(
          "ConnectionConfig cannot be null. Please use setConnectionConfig()");
    }

    if (connectionConfig.getHost() == null || connectionConfig.getPort() <= 0) {
      throw new IllegalArgumentException(
          "host and port needs to be specified in order to build the service.");
    }

    DefaultUriTemplateHandler templateHandler = new DefaultUriTemplateHandler();
    String schema = (connectionConfig.getSslContext() == null) ? "http" : "https";
    templateHandler
        .setBaseUrl(schema + "://" + connectionConfig.getHost() + ":" + connectionConfig.getPort()
            + "/management");
    restTemplate.setUriTemplateHandler(templateHandler);

    // HttpComponentsClientHttpRequestFactory allows use to preconfigure httpClient for
    // authentication and ssl context
    HttpComponentsClientHttpRequestFactory requestFactory =
        new HttpComponentsClientHttpRequestFactory();

    HttpClientBuilder clientBuilder = HttpClientBuilder.create();
    // configures the clientBuilder
    if (connectionConfig.getAuthToken() != null) {
      List<Header> defaultHeaders = Arrays.asList(
          new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + connectionConfig.getAuthToken()));
      clientBuilder.setDefaultHeaders(defaultHeaders);
    } else if (connectionConfig.getUsername() != null) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(
          new AuthScope(connectionConfig.getHost(), connectionConfig.getPort()),
          new UsernamePasswordCredentials(connectionConfig.getUsername(),
              connectionConfig.getPassword()));
      clientBuilder.setDefaultCredentialsProvider(credsProvider);
    }

    clientBuilder.setSSLContext(connectionConfig.getSslContext());
    clientBuilder.setSSLHostnameVerifier(connectionConfig.getHostnameVerifier());

    requestFactory.setHttpClient(clientBuilder.build());
    restTemplate.setRequestFactory(requestFactory);

    // configure our own ObjectMapper
    MappingJackson2HttpMessageConverter messageConverter =
        new MappingJackson2HttpMessageConverter();
    messageConverter.setPrettyPrint(false);
    // the client should use a mapper that would ignore unknown properties in case the server
    // is a newer version than the client
    messageConverter.setObjectMapper(GeodeJsonMapper.getMapperIgnoringUnknownProperties());
    restTemplate.getMessageConverters().removeIf(
        m -> m.getClass().getName().equals(MappingJackson2HttpMessageConverter.class.getName()));
    restTemplate.getMessageConverters().add(messageConverter);
  }

  @Override
  public <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult submitMessage(
      T configMessage, CommandType command,
      Class<? extends ClusterManagementRealizationResult> responseType) {
    switch (command) {
      case CREATE:
        return create(configMessage, responseType);
      case DELETE:
        return delete(configMessage, responseType);
    }

    throw new IllegalArgumentException("Unable to process command " + command
        + ". Perhaps you need to use a different method in ClusterManagementServiceTransport.");
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementGetResult<T, R> submitMessageForGet(
      T config, Class<? extends ClusterManagementGetResult> responseType) {
    return restTemplate.exchange(getIdentityEndpoint(config), HttpMethod.GET, makeEntity(config),
        responseType)
        .getBody();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends AbstractConfiguration<R>, R extends RuntimeInfo> ClusterManagementListResult<T, R> submitMessageForList(
      T config, Class<? extends ClusterManagementListResult> responseType) {
    String endPoint = URI_VERSION + config.getLinks().getList();
    return restTemplate
        .exchange(endPoint + "/?id={id}&group={group}", HttpMethod.GET, makeEntity(config),
            responseType, config.getId(), config.getGroup())
        .getBody();
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementListOperationsResult<V> submitMessageForListOperation(
      A opType, Class<? extends ClusterManagementListOperationsResult> responseType) {
    final ClusterManagementListOperationsResult<V> result;

    // make the REST call to list in-progress operations
    result = assertSuccessful(restTemplate
        .exchange(URI_VERSION + opType.getEndpoint(), HttpMethod.GET,
            makeEntity(null), ClusterManagementListOperationsResult.class)
        .getBody());

    return new ClusterManagementListOperationsResult<>(
        result.getResult().stream().map(r -> reAnimate(r, opType.getEndpoint()))
            .collect(Collectors.toList()));
  }

  @Override
  public <A extends ClusterManagementOperation<V>, V extends OperationResult> ClusterManagementOperationResult<V> submitMessageForStart(
      A op) {
    final ClusterManagementOperationResult result;

    // make the REST call to start the operation
    result = assertSuccessful(restTemplate
        .exchange(URI_VERSION + op.getEndpoint(), HttpMethod.POST, makeEntity(op),
            ClusterManagementOperationResult.class)
        .getBody());

    // our restTemplate requires the url to be modified to start from "/v1"
    return reAnimate(result, op.getEndpoint());
  }

  @Override
  public boolean isConnected() {
    try {
      return restTemplate.getForEntity(URI_VERSION + "/ping", String.class)
          .getBody().equals("pong");
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void close() {
    longRunningStatusPollingThreadPool.shutdownNow();
  }

  private <V extends OperationResult> ClusterManagementOperationResult<V> reAnimate(
      ClusterManagementOperationResult<V> result, String endPoint) {
    String uri = URI_VERSION + endPoint + "/" + result.getOperationId();

    // complete the future by polling the check-status REST endpoint
    CompletableFuture<Date> futureOperationEnded = new CompletableFuture<>();
    CompletableFutureProxy<V> operationResult =
        new CompletableFutureProxy<>(restTemplate, uri, longRunningStatusPollingThreadPool,
            futureOperationEnded);

    return new ClusterManagementOperationResult<>(result, operationResult,
        result.getOperationStart(), futureOperationEnded, result.getOperator(),
        result.getOperationId());
  }


  private <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult create(T config,
      Class<? extends ClusterManagementRealizationResult> expectedResult) {
    String endPoint = URI_VERSION + config.getLinks().getList();
    // the response status code info is represented by the ClusterManagementResult.errorCode already
    return restTemplate.exchange(endPoint, HttpMethod.POST, makeEntity(config),
        expectedResult)
        .getBody();
  }

  private <T extends AbstractConfiguration<?>> ClusterManagementRealizationResult delete(
      T config, Class<? extends ClusterManagementRealizationResult> expectedResult) {
    String uri = getIdentityEndpoint(config);
    return restTemplate.exchange(uri + "?group={group}",
        HttpMethod.DELETE,
        makeEntity(null),
        expectedResult,
        config.getGroup())
        .getBody();
  }

  public static <T> HttpEntity<T> makeEntity(T config) {
    HttpHeaders headers = new HttpHeaders();
    headers.add(INCLUDE_CLASS_HEADER, "true");
    return new HttpEntity<>(config, headers);
  }

  private String getIdentityEndpoint(AbstractConfiguration<?> config) {
    String uri = config.getLinks().getSelf();
    if (uri == null) {
      throw new IllegalArgumentException(
          "Unable to construct the URI with the current configuration.");
    }
    return URI_VERSION + uri;
  }

  private <T extends ClusterManagementResult> T assertSuccessful(T result) {
    if (result == null) {
      ClusterManagementResult somethingVeryBadHappened = new ClusterManagementResult(
          ClusterManagementResult.StatusCode.ERROR, "Unable to parse server response.");
      throw new ClusterManagementException(somethingVeryBadHappened);
    } else if (!result.isSuccessful()) {
      throw new ClusterManagementException(result);
    }
    return result;
  }
}
