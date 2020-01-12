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

import static org.apache.geode.management.internal.ClientClusterManagementService.makeEntity;

import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.runtime.OperationResult;

@Experimental
public class CompletableFutureProxy<V extends OperationResult> extends CompletableFuture<V>
    implements Dormant {
  private static final int POLL_INTERVAL = 1000; // millis between http status checks

  private static final int TOLERABLE_FAILURES = 3;
  private int consecutiveCheckFailures = 0;

  private final ScheduledExecutorService pool;
  private final RestTemplate restTemplate;
  private final String uri;
  private ScheduledFuture<?> scheduledFuture;
  private final CompletableFuture<Date> futureOperationEnded;

  public CompletableFutureProxy(RestTemplate restTemplate, String uri,
      ScheduledExecutorService pool, CompletableFuture<Date> futureOperationEnded) {
    this.restTemplate = restTemplate;
    this.uri = uri;
    this.pool = pool;
    this.futureOperationEnded = futureOperationEnded;
  }

  @Override
  public void wakeUp() {
    startPolling();
  }

  private synchronized void startPolling() {
    if (scheduledFuture != null) {
      return;
    }
    scheduledFuture = pool.scheduleWithFixedDelay(() -> {
      // bail out if the future has already been completed (successfully by us, or by user cancel,
      // or by exception)
      if (isDone()) {
        futureOperationEnded.complete(null);
        scheduledFuture.cancel(true);
        return;
      }

      // get the current status via REST call
      ClusterManagementOperationStatusResult<V> result;
      try {
        result = requestStatus();
        consecutiveCheckFailures = 0;
      } catch (Exception e) {
        // don't panic if a few checks fail, might just be network issue
        if (++consecutiveCheckFailures > TOLERABLE_FAILURES) {
          futureOperationEnded.complete(null);
          completeExceptionally(new RuntimeException("Lost connectivity to locator " + e));
        }
        return;
      }

      completeAccordingToStatus(result);
    }, 0, POLL_INTERVAL, TimeUnit.MILLISECONDS);
  }

  private void completeAccordingToStatus(ClusterManagementOperationStatusResult<V> result) {
    ClusterManagementResult.StatusCode statusCode = result.getStatusCode();
    if (statusCode == ClusterManagementResult.StatusCode.OK) {
      futureOperationEnded.complete(result.getOperationEnded());
      complete(result.getResult());
    } else if (statusCode == ClusterManagementResult.StatusCode.IN_PROGRESS) {
      // do nothing
    } else {
      futureOperationEnded.complete(null);
      completeExceptionally(
          new RuntimeException(statusCode + ": " + result.getStatusMessage()));
    }
  }

  /**
   * this should be the only method to make the request to the locator to check the status of the
   * operation
   */
  @SuppressWarnings("unchecked")
  private ClusterManagementOperationStatusResult<V> requestStatus() {
    return restTemplate
        .exchange(uri, HttpMethod.GET, makeEntity(null),
            ClusterManagementOperationStatusResult.class)
        .getBody();
  }
}
