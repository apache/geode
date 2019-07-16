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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.web.client.RestTemplate;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementOperationStatusResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.JsonSerializable;

@Experimental
public class FutureProxy<V extends JsonSerializable> implements Future<V> {
  static final int POLL_INTERVAL = 100; // millis between http status checks
  static final ExecutorService pool = Executors.newFixedThreadPool(10);
  private RestTemplate restTemplate;
  private String uri;
  private volatile boolean cancelled = false;

  // cache the first "done" response to avoid further http calls
  private ClusterManagementOperationStatusResult<V> completedResult = null;

  public FutureProxy(RestTemplate restTemplate, String uri) {
    this.restTemplate = restTemplate;
    this.uri = uri;
  }

  /**
   * converts this Future into a CompletableFuture
   */
  public CompletableFuture<V> toCompletableFuture() {
    CompletableFuture<V> future = new CompletableFutureAdapter<>();
    pool.execute(() -> {
      try {
        future.complete(get());
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    });
    return future;
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

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    completedResult = new ClusterManagementOperationStatusResult<V>(
        ClusterManagementResult.StatusCode.ERROR,
        "Future.cancel() invoked.  this does not affect the operation; only stops waiting on it.");
    cancelled = true;
    return true;
  }

  @Override
  public boolean isCancelled() {
    return cancelled;
  }

  @Override
  public boolean isDone() {
    if (completedResult != null) {
      return true;
    }
    ClusterManagementOperationStatusResult<V> result = requestStatus();
    boolean done = result.getStatusCode() != ClusterManagementResult.StatusCode.IN_PROGRESS;
    if (done) {
      completedResult = result;
    }
    return done;
  }

  @Override
  public V get() throws InterruptedException, ExecutionException {
    while (!isDone()) {
      Thread.sleep(POLL_INTERVAL);
    }
    return completedResult.getResult();
  }

  @Override
  public V get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    long timeoutMillis = unit.toMillis(timeout);
    long startTime = System.currentTimeMillis();
    while (!isDone()) {
      long elapsedTime = System.currentTimeMillis() - startTime;
      if (elapsedTime > timeoutMillis) {
        throw new TimeoutException();
      }
      Thread.sleep(POLL_INTERVAL);
    }
    return completedResult.getResult();
  }

  class CompletableFutureAdapter<V extends JsonSerializable> extends CompletableFuture<V> {
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      super.cancel(mayInterruptIfRunning);
      FutureProxy.this.cancelled = true;
      return true;
    }
  }
}
