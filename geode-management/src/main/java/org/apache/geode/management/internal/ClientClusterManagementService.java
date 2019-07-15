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


import org.apache.commons.lang3.NotImplementedException;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.management.api.AsyncOperation;
import org.apache.geode.management.api.ClusterManagementListResult;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.CorrespondWith;
import org.apache.geode.management.api.JsonSerializable;
import org.apache.geode.management.api.RestfulEndpoint;
import org.apache.geode.management.runtime.RuntimeInfo;

/**
 * Implementation of {@link ClusterManagementService} interface which represents the cluster
 * management service as used by a Java client.
 * <p/>
 * In order to manipulate Geode components (Regions, etc.) clients can construct instances of {@link
 * CacheElement}s and call the corresponding
 * {@link ClientClusterManagementService#create(CacheElement)},
 * {@link ClientClusterManagementService#delete(CacheElement)} or
 * {@link ClientClusterManagementService#update(CacheElement)} method. The returned {@link
 * ClusterManagementResult} will contain all necessary information about the outcome of the call.
 * This will include the result of persisting the config as part of the cluster configuration as
 * well as creating the actual component in the cluster.
 * <p/>
 * All create calls are idempotent and will not return an error if the desired component already
 * exists.
 */
public class ClientClusterManagementService implements ClusterManagementService {
  // the restTemplate needs to have the context as the baseUrl, and request URI is the part after
  // the context (including /v2), it needs to be set up this way so that spring test runner's
  // injected RequestFactory can work
  private final RestTemplate restTemplate;

  ClientClusterManagementService(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends CacheElement> ClusterManagementResult create(T config) {
    String endPoint = getEndpoint(config);
    // the response status code info is represented by the ClusterManagementResult.errorCode already
    return restTemplate
        .postForEntity(endPoint, config, ClusterManagementResult.class)
        .getBody();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends CacheElement> ClusterManagementResult delete(
      T config) {
    String uri = getIdentityEndPoint(config);
    return restTemplate
        .exchange(uri + "?group={group}",
            HttpMethod.DELETE,
            null,
            ClusterManagementResult.class,
            config.getGroup())
        .getBody();
  }

  @Override
  public <T extends CacheElement> ClusterManagementResult update(
      T config) {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends CacheElement & CorrespondWith<R>, R extends RuntimeInfo> ClusterManagementListResult<T, R> list(
      T config) {
    String endPoint = getEndpoint(config);
    return restTemplate
        .getForEntity(endPoint + "/?id={id}&group={group}",
            ClusterManagementListResult.class, config.getId(), config.getGroup())
        .getBody();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends CacheElement & CorrespondWith<R>, R extends RuntimeInfo> ClusterManagementListResult<T, R> get(
      T config) {
    return restTemplate
        .getForEntity(getIdentityEndPoint(config), ClusterManagementListResult.class)
        .getBody();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <A extends AsyncOperation<V>, V extends JsonSerializable> ClusterManagementOperationResult<V> startOperation(
      A op) {
    ClusterManagementOperationResult<V> result =
        restTemplate.postForEntity(RestfulEndpoint.URI_VERSION + op.getEndpoint(), op,
            ClusterManagementOperationResult.class).getBody();
    ClientAsyncOperationResult<V> operationResult =
        new ClientAsyncOperationResult<>(restTemplate,
            stripPrefix(RestfulEndpoint.URI_CONTEXT, result.getUri()));
    result.setOperationResult(operationResult);
    return result;
  }

  private static String stripPrefix(String prefix, String s) {
    if (s.startsWith(prefix)) {
      return s.substring(prefix.length());
    }
    return s;
  }

  private String getEndpoint(CacheElement config) {
    checkIsRestful(config);
    String endpoint = ((RestfulEndpoint) config).getEndpoint();
    if (endpoint == null) {
      throw new IllegalArgumentException(
          "unable to construct the uri with the current configuration.");
    }
    return RestfulEndpoint.URI_VERSION + endpoint;
  }

  private String getIdentityEndPoint(CacheElement config) {
    checkIsRestful(config);
    String uri = ((RestfulEndpoint) config).getIdentityEndPoint();
    if (uri == null) {
      throw new IllegalArgumentException(
          "unable to construct the uri with the current configuration.");
    }
    return RestfulEndpoint.URI_VERSION + uri;
  }

  private void checkIsRestful(CacheElement config) {
    if (!(config instanceof RestfulEndpoint)) {
      throw new IllegalArgumentException(
          String.format("The config type %s does not have a RESTful endpoint defined",
              config.getClass().getName()));
    }
  }

  public boolean isConnected() {
    return restTemplate.getForEntity(RestfulEndpoint.URI_VERSION + "/ping", String.class)
        .getBody().equals("pong");
  }

}
