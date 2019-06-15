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
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.RespondsWith;
import org.apache.geode.management.api.RestfulEndpoint;

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
  private static final String VERSION = "/v2";

  private final RestTemplate restTemplate;

  ClientClusterManagementService(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends CacheElement & RespondsWith<R>, R extends CacheElement> ClusterManagementResult<T> create(
      T config) {
    String endPoint = getEndpoint(config);
    // the response status code info is represented by the ClusterManagementResult.errorCode already
    return restTemplate
        .postForEntity(VERSION + endPoint, config, ClusterManagementResult.class)
        .getBody();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends CacheElement & RespondsWith<R>, R extends CacheElement> ClusterManagementResult<T> delete(
      T config) {
    String uri = getUri(config);
    return restTemplate
        .exchange(VERSION + uri + "?group={group}",
            HttpMethod.DELETE,
            null,
            ClusterManagementResult.class,
            config.getGroup())
        .getBody();
  }

  @Override
  public <T extends CacheElement & RespondsWith<R>, R extends CacheElement> ClusterManagementResult<T> update(
      T config) {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends CacheElement & RespondsWith<R>, R extends CacheElement> ClusterManagementResult<R> list(
      T config) {
    String endPoint = getEndpoint(config);
    return restTemplate
        .getForEntity(VERSION + endPoint + "/?id={id}&group={group}",
            ClusterManagementResult.class, config.getId(), config.getGroup())
        .getBody();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends CacheElement & RespondsWith<R>, R extends CacheElement> ClusterManagementResult<R> get(
      T config) {
    return restTemplate
        .getForEntity(VERSION + getUri(config), ClusterManagementResult.class)
        .getBody();
  }

  private String getEndpoint(CacheElement config) {
    checkIsRestful(config);
    String endpoint = ((RestfulEndpoint) config).getEndpoint();
    if (endpoint == null) {
      throw new IllegalArgumentException(
          "unable to construct the uri with the current configuration.");
    }
    return endpoint;
  }

  private String getUri(CacheElement config) {
    checkIsRestful(config);
    String uri = ((RestfulEndpoint) config).getUri();
    if (uri == null) {
      throw new IllegalArgumentException(
          "unable to construct the uri with the current configuration.");
    }
    return uri;
  }

  private void checkIsRestful(CacheElement config) {
    if (!(config instanceof RestfulEndpoint)) {
      throw new IllegalArgumentException(
          String.format("The config type %s does not have a RESTful endpoint defined",
              config.getClass().getName()));
    }
  }

  public boolean isConnected() {
    return restTemplate.getForEntity(VERSION + "/ping", String.class)
        .getBody().equals("pong");
  }

}
