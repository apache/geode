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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.web.client.RestTemplate;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementOperationStatusResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.JsonSerializable;

@Experimental
public class CompletableFutureProxy<V extends JsonSerializable> extends CompletableFuture<V> {
  static final int POLL_INTERVAL = 100; // millis between http status checks
  static final ExecutorService pool = Executors.newFixedThreadPool(10);
  private RestTemplate restTemplate;
  private String uri;

  public CompletableFutureProxy(RestTemplate restTemplate, String uri) {
    this.restTemplate = restTemplate;
    this.uri = uri;
  }

  public void startPolling(){
    pool.execute(() -> {
      try {
        complete(getImpl());
      } catch (Throwable t) {
        completeExceptionally(t);
      }
    });
  }

  /**
   * this should be the only method to make the request to the locator to check the status of the
   * operation
   */
  @SuppressWarnings("unchecked")
  private ClusterManagementOperationStatusResult<V> requestStatus() {
    return restTemplate
        .getForEntity(uri, ClusterManagementOperationStatusResult.class)
        .getBody();
  }

  private V getImpl() throws InterruptedException, ExecutionException {
    while (true) {
      if (isDone())
        return null;
      ClusterManagementOperationStatusResult<V>  result = requestStatus();
      boolean done = result.getStatusCode() != ClusterManagementResult.StatusCode.IN_PROGRESS;
      if (!done)
        Thread.sleep(POLL_INTERVAL);
      else
        return result.getResult();
    }
  }
}
