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

import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriTemplateHandler;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.GeodeManagementException;
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

  private final RestTemplate restTemplate;

  private ClientClusterManagementService() {
    restTemplate = new RestTemplate();
    restTemplate.setErrorHandler(DEFAULT_ERROR_HANDLER);
  }

  public ClientClusterManagementService(ClientHttpRequestFactory requestFactory) {
    this();
    this.restTemplate.setRequestFactory(requestFactory);
  }

  public ClientClusterManagementService(String clusterUrl) {
    this();
    DefaultUriTemplateHandler templateHandler = new DefaultUriTemplateHandler();
    templateHandler.setBaseUrl(clusterUrl);
    restTemplate.setUriTemplateHandler(templateHandler);
  }

  @Override
  public ClusterManagementResult create(CacheElement config, String group) {
    String endPoint = getEndpoint(config);

    ClusterManagementResult result = restTemplate.postForEntity(endPoint, config,
        ClusterManagementResult.class).getBody();

    String exceptionClass = result.getPersistenceStatus().getExceptionClass();

    if (exceptionClass != null) {
      throw new GeodeManagementException(exceptionClass,
          result.getPersistenceStatus().getMessage());
    }
    return result;
  }

  @Override
  public ClusterManagementResult delete(CacheElement config, String group) {
    throw new NotImplementedException();
  }

  @Override
  public ClusterManagementResult update(CacheElement config, String group) {
    throw new NotImplementedException();
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
}
