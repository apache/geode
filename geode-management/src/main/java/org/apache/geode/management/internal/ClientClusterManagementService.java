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
import org.apache.geode.management.api.CMSJsonSerializable;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.Groupable;
import org.apache.geode.management.api.RespondsWith;
import org.apache.geode.management.api.RestfulEndpoint;

/**
 * Implementation of {@link ClusterManagementService} interface which represents the cluster
 * management service as used by a Java client.
 * <p/>
 * In order to manipulate Geode components (Regions, etc.) clients can construct instances of {@link
 * CacheElement}s and call the corresponding
 * {@link ClientClusterManagementService#create(RestfulEndpoint)},
 * {@link ClientClusterManagementService#delete(RestfulEndpoint)} or
 * {@link ClientClusterManagementService#update(RestfulEndpoint)} method. The returned {@link
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
  public <T extends RestfulEndpoint & RespondsWith<R>, R extends CMSJsonSerializable> ClusterManagementResult<T> create(
      T config) {
    String endPoint = getEndpoint(config);
    // the response status code info is represented by the ClusterManagementResult.errorCode already
    return restTemplate
        .postForEntity(VERSION + endPoint, config, ClusterManagementResult.class)
        .getBody();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends RestfulEndpoint & RespondsWith<R>, R extends CMSJsonSerializable> ClusterManagementResult<T> delete(
      T config) {
    String uri = getUri(config);
    return restTemplate
        .exchange(VERSION + uri + "?group={group}",
            HttpMethod.DELETE,
            null,
            ClusterManagementResult.class,
            getGroup(config))
        .getBody();
  }

  @Override
  public <T extends RestfulEndpoint & RespondsWith<R>, R extends CMSJsonSerializable> ClusterManagementResult<T> update(
      T config) {
    throw new NotImplementedException("Not Implemented");
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends RestfulEndpoint & RespondsWith<R>, R extends CMSJsonSerializable> ClusterManagementResult<R> list(
      T config) {
    String endPoint = getEndpoint(config);
    return restTemplate
        .getForEntity(VERSION + endPoint + "/?id={id}&group={group}",
            ClusterManagementResult.class, config.getId(), getGroup(config))
        .getBody();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends RestfulEndpoint & RespondsWith<R>, R extends CMSJsonSerializable> ClusterManagementResult<R> get(
      T config) {
    return restTemplate
        .getForEntity(VERSION + getUri(config), ClusterManagementResult.class)
        .getBody();
  }

  private String getEndpoint(RestfulEndpoint config) {
    String endpoint = config.getEndpoint();
    if (endpoint == null) {
      throw new IllegalArgumentException(
          "unable to construct the uri with the current configuration.");
    }
    return endpoint;
  }

  private String getUri(RestfulEndpoint config) {
    String uri = config.getUri();
    if (uri == null) {
      throw new IllegalArgumentException(
          "unable to construct the uri with the current configuration.");
    }
    return uri;
  }

  private static String getGroup(Object config) {
    if (config instanceof Groupable) {
      return ((Groupable) config).getGroup();
    } else {
      return null;
    }
  }

  public boolean isConnected() {
    return restTemplate.getForEntity(VERSION + "/ping", String.class)
        .getBody().equals("pong");
  }

}
