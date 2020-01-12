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

import static org.apache.geode.management.internal.PlainClusterManagementServiceBuilder.DEFAULT_ERROR_HANDLER;

import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;

public class SpringClusterManagemnetServiceBuilder implements
    ClusterManagementServiceBuilder.HttpRequestFactoryBuilder {

  protected ClientHttpRequestFactory requestFactory;

  public ClusterManagementService build() {
    RestTemplate restTemplate = new RestTemplate();
    restTemplate.setErrorHandler(DEFAULT_ERROR_HANDLER);
    restTemplate.setRequestFactory(requestFactory);
    return new ClientClusterManagementService(restTemplate);
  }

  public SpringClusterManagemnetServiceBuilder setRequestFactory(
      ClientHttpRequestFactory httpRequestFactory) {
    this.requestFactory = httpRequestFactory;
    return this;
  }
}
