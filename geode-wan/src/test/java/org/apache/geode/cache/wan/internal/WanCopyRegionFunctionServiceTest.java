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
package org.apache.geode.cache.wan.internal;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class WanCopyRegionFunctionServiceTest {

  private WanCopyRegionFunctionService service;
  private final InternalCache cache = mock(InternalCache.class);

  @BeforeEach
  public void setUp() throws Exception {
    service = new WanCopyRegionFunctionService();
    service.init(cache);
  }

  @Test
  public void severalExecuteWithSameRegionAndSenderNotAllowed() {
    CountDownLatch latch = new CountDownLatch(1);
    Callable<CliFunctionResult> firstExecution = () -> {
      latch.await(GeodeAwaitility.getTimeout().getSeconds(), TimeUnit.SECONDS);
      return null;
    };

    String regionName = "myRegion";
    String senderId = "mySender";
    CompletableFuture
        .supplyAsync(() -> {
          try {
            return service.execute(firstExecution, regionName, senderId);
          } catch (Exception e) {
            return null;
          }
        });

    // Wait for the execute function to start
    await().untilAsserted(() -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(1));

    // Execute another function instance for the same region and sender-id
    Callable<CliFunctionResult> secondExecution = () -> null;

    assertThatThrownBy(() -> service.execute(secondExecution, regionName, senderId))
        .isInstanceOf(WanCopyRegionFunctionServiceAlreadyRunningException.class);

    // Let first execution finish
    latch.countDown();
  }

  @Test
  public void cancelRunningExecutionReturnsSuccess() {
    String regionName = "myRegion";
    String senderId = "mySender";
    CountDownLatch latch = new CountDownLatch(1);
    Callable<CliFunctionResult> firstExecution = () -> {
      latch.await(GeodeAwaitility.getTimeout().getSeconds(), TimeUnit.SECONDS);
      return null;
    };
    CompletableFuture
        .supplyAsync(() -> {
          try {
            return service.execute(firstExecution, regionName, senderId);
          } catch (Exception e) {
            return null;
          }
        });

    // Wait for the function to start execution
    await().untilAsserted(() -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(1));

    // Cancel the function execution
    boolean result = service.cancel(regionName, senderId);

    assertThat(result).isEqualTo(true);
    await().untilAsserted(() -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(0));
  }

  @Test
  public void cancelNotRunningExecutionReturnsError() {
    final String regionName = "myRegion";
    final String senderId = "mySender";

    boolean result = service.cancel(regionName, senderId);

    assertThat(result).isEqualTo(false);
  }

  @Test
  public void cancelAllExecutionsWithRunningExecutionsReturnsCanceledExecutions() {
    int executions = 2;
    CountDownLatch latch = new CountDownLatch(executions);
    ExecutorService executorService = Executors.newFixedThreadPool(executions);
    Callable<CliFunctionResult> firstExecution = () -> {
      latch.await(GeodeAwaitility.getTimeout().getSeconds(), TimeUnit.SECONDS);
      return null;
    };

    executorService.submit(() -> {
      try {
        return service.execute(firstExecution, "myRegion", "mySender1");
      } catch (Exception e) {
        return null;
      }
    });

    Callable<CliFunctionResult> secondExecution = () -> {
      latch.await(GeodeAwaitility.getTimeout().getSeconds(), TimeUnit.SECONDS);
      return null;
    };

    executorService.submit(() -> {
      try {
        return service.execute(secondExecution, "myRegion", "mySender");
      } catch (Exception e) {
        return null;
      }
    });

    // Wait for the functions to start execution
    await().untilAsserted(
        () -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(executions));

    // Cancel the function execution
    String executionsString = service.cancelAll();

    assertThat(executionsString).isEqualTo("[(myRegion,mySender1), (myRegion,mySender)]");
    await().untilAsserted(() -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(0));
    executorService.shutdown();
  }

  @Test
  public void severalExecuteWithDifferentRegionOrSenderAreAllowed() {
    int executions = 5;
    CountDownLatch latch = new CountDownLatch(executions);
    ExecutorService executorService = Executors.newFixedThreadPool(executions);
    for (int i = 0; i < executions; i++) {
      Callable<CliFunctionResult> execution = () -> {
        latch.await(GeodeAwaitility.getTimeout().getSeconds(), TimeUnit.SECONDS);
        return null;
      };

      final String regionName = String.valueOf(i);
      executorService.submit(() -> {
        try {
          return service.execute(execution, regionName, "mySender1");
        } catch (Exception e) {
          return null;
        }
      });
    }

    // Wait for the functions to start execution
    await().untilAsserted(
        () -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(executions));

    // End executions
    for (int i = 0; i < executions; i++) {
      latch.countDown();
    }
    executorService.shutdown();
  }

  @Test
  public void concurrentExecutionsDoesNotExceedMaxConcurrentExecutions() {
    int maxConcurrentExecutions = 2;
    service.init(maxConcurrentExecutions);

    int executions = 4;
    CountDownLatch latch = new CountDownLatch(executions);
    AtomicInteger concurrentExecutions = new AtomicInteger(0);
    ExecutorService executorService = Executors.newFixedThreadPool(executions);
    for (int i = 0; i < executions; i++) {
      Callable<CliFunctionResult> execution = () -> {
        concurrentExecutions.incrementAndGet();
        latch.await(GeodeAwaitility.getTimeout().getSeconds(), TimeUnit.SECONDS);
        concurrentExecutions.decrementAndGet();
        return null;
      };

      final String regionName = String.valueOf(i);
      executorService.submit(() -> {
        try {
          return service.execute(execution, regionName, "mySender1");
        } catch (Exception e) {
          return null;
        }
      });
    }

    // Wait for the functions to start execution
    await().untilAsserted(
        () -> assertThat(service.getNumberOfCurrentExecutions()).isEqualTo(executions));

    // Make sure concurrent executions does not exceed the maximum
    assertThat(concurrentExecutions.get()).isEqualTo(maxConcurrentExecutions);

    // End executions
    for (int i = 0; i < executions; i++) {
      latch.countDown();
    }

    await().untilAsserted(() -> assertThat(concurrentExecutions.get()).isEqualTo(0));
    executorService.shutdown();
  }

}
