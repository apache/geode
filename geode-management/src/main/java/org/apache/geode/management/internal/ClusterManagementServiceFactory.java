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

import org.apache.geode.management.api.ClusterManagementService;

/**
 * A Server Provider interface which should be implemented to create instances of {@link
 * ClusterManagementService} for a given context.
 */
public interface ClusterManagementServiceFactory {

  /**
   * Returns the context of the factory
   */
  String getContext();

  /**
   * Create a {@code ClusterManagementService} instance. Throws an {@code IllegalStateException}
   * if the instance cannot be created. This method would typically be used by an implementation
   * that is able to infer the correct Cluster Management endpoint URL to use.
   *
   * @return an instance of the {@code ClusterManagementService}
   */
  ClusterManagementService create() throws IllegalStateException;

  /**
   * Create a {@code ClusterManagementService} that will use the given URL.
   *
   * @param clusterUrl the URL to conect to
   * @return an instance of {@code ClusterManagementService}
   */
  ClusterManagementService create(String clusterUrl);

  /**
   * Create a {@code ClusterManagementService} that will use the given {@link
   * ClientHttpRequestFactory} to establish connections with the Cluster Management endpoint.
   *
   * @param requestFactory the {@code ClientHttpRequestFactory} to use for new connections.
   * @return an instance of {@code ClusterManagementService}
   */
  ClusterManagementService create(ClientHttpRequestFactory requestFactory);
}
