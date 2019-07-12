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
package org.apache.geode.management.api;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.web.client.RestTemplate;

import org.apache.geode.annotations.Experimental;

@Experimental
public class ClientAsyncOperationResult<V extends JsonSerializable>
    extends AsyncOperationResult<V> {
  private RestTemplate restTemplate;
  private String uri;

  public ClientAsyncOperationResult(RestTemplate restTemplate, String uri) {
    this.restTemplate = restTemplate;
    this.uri = uri;
  }

  @SuppressWarnings("unchecked")
  /**
   * this should be the only method to make the request to the locator to check the status of the
   * operation
   */
  private ClusterManagementOperationResult<V> requestStatus() {
    return restTemplate
        .getForEntity(uri, ClusterManagementOperationResult.class)
        .getBody();
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    ClusterManagementOperationResult<V> result = requestStatus();
    return result.getStatusCode() != ClusterManagementResult.StatusCode.IN_PROGRESS;
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    while (!isDone()) {
      Thread.sleep(100);
    }
    ClusterManagementOperationResult<V> result = requestStatus();
    return result.getOperationResult().get();
  }

  @Override
  public V get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    long timeoutMillis = unit.toMillis(timeout);
    long startTime = System.currentTimeMillis();
    while (!isDone()) {
      long elapsedTime = System.currentTimeMillis() - startTime;
      if (elapsedTime > timeoutMillis)
        throw new TimeoutException();
      Thread.sleep(100);
    }


    ClusterManagementOperationResult<V> result = requestStatus();
    return result.getOperationResult().get();
  }
}
