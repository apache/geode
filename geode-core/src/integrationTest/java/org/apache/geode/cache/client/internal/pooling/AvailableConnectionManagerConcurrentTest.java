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

package org.apache.geode.cache.client.internal.pooling;

import static org.apache.geode.test.concurrency.Utilities.availableProcessors;
import static org.apache.geode.test.concurrency.Utilities.repeat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.test.concurrency.ConcurrentTestRunner;
import org.apache.geode.test.concurrency.ParallelExecutor;

@RunWith(ConcurrentTestRunner.class)
public class AvailableConnectionManagerConcurrentTest {
  private final int parallelCount = availableProcessors();
  private final int iterationCount = 250;
  private final AvailableConnectionManager instance = new AvailableConnectionManager();

  @Test
  public void useFirstAddFirstDoesNotLoseConnections(ParallelExecutor executor)
      throws ExecutionException, InterruptedException {
    repeat(() -> instance.addFirst(createConnection(), false), parallelCount);

    executor.inParallel(() -> {
      repeat(() -> {
        PooledConnection used = instance.useFirst();
        instance.addFirst(used, true);
      }, iterationCount);
    }, parallelCount);
    executor.execute();

    assertThat(instance.getDeque()).hasSize(parallelCount);
  }

  @Test
  public void useFirstWithPredicateAddFirstDoesNotLoseConnections(ParallelExecutor executor)
      throws ExecutionException, InterruptedException {
    repeat(() -> instance.addFirst(createConnection(), false), parallelCount);

    executor.inParallel(() -> {
      repeat(() -> {
        PooledConnection used = instance.useFirst(c -> true);
        instance.addFirst(used, true);
      }, iterationCount);
    }, parallelCount);
    executor.execute();

    assertThat(instance.getDeque()).hasSize(parallelCount);
  }

  @Test
  public void useFirstAddLastDoesNotLoseConnections(ParallelExecutor executor)
      throws ExecutionException, InterruptedException {
    repeat(() -> instance.addFirst(createConnection(), false), parallelCount);

    executor.inParallel(() -> {
      repeat(() -> {
        PooledConnection used = instance.useFirst();
        instance.addLast(used, true);
      }, iterationCount);
    }, parallelCount);
    executor.execute();

    assertThat(instance.getDeque()).hasSize(parallelCount);
  }

  @Test
  public void useFirstAddFirstDoesNotLoseConnectionsEvenWhenUseFirstReturnsNull(
      ParallelExecutor executor)
      throws ExecutionException, InterruptedException {
    int connectionCount = 2;
    int threadCount = connectionCount * 5;
    repeat(() -> instance.addFirst(createConnection(), false), connectionCount);

    executor.inParallel(() -> {
      repeat(() -> {
        PooledConnection used = instance.useFirst();
        if (used != null) {
          Thread.yield();
          instance.addFirst(used, true);
        }
      }, iterationCount);
    }, threadCount);
    executor.execute();

    assertThat(instance.getDeque()).hasSize(connectionCount);
  }

  @Test
  public void useFirstWithPredicateAddFirstDoesNotLoseConnectionsEvenWhenUseFirstReturnsNull(
      ParallelExecutor executor)
      throws ExecutionException, InterruptedException {
    int connectionCount = 2;
    int threadCount = connectionCount * 5;
    repeat(() -> instance.addFirst(createConnection(), false), connectionCount);

    executor.inParallel(() -> {
      repeat(() -> {
        PooledConnection used = instance.useFirst(c -> true);
        if (used != null) {
          Thread.yield();
          instance.addFirst(used, true);
        }
      }, iterationCount);
    }, threadCount);
    executor.execute();

    assertThat(instance.getDeque()).hasSize(connectionCount);
  }

  @Test
  public void useFirstAddLastWithPredicateThatDoesNotAlwaysMatchDoesNotLoseConnectionsEvenWhenUseFirstReturnsNull(
      ParallelExecutor executor)
      throws ExecutionException, InterruptedException {
    int connectionCount = 2;
    int threadCount = connectionCount * 5;
    repeat(() -> instance.addFirst(createConnection(), false), connectionCount);
    // now add a bunch of connections that will not match the predicate
    repeat(() -> {
      PooledConnection nonMatchingConnection = createConnection();
      when(nonMatchingConnection.getBirthDate()).thenReturn(1L);
      instance.addFirst(nonMatchingConnection, false);
    }, connectionCount);

    executor.inParallel(() -> {
      repeat(() -> {
        PooledConnection used = instance.useFirst(c -> c.getBirthDate() == 0L);
        if (used != null) {
          Thread.yield();
          assertThat(used.getBirthDate()).isEqualTo(0L);
          instance.addLast(used, true);
        }
      }, iterationCount);
    }, threadCount);
    executor.execute();

    assertThat(instance.getDeque()).hasSize(connectionCount * 2);
  }

  @Test
  public void addLastRemoveDoesNotRemoveOtherConnections(ParallelExecutor executor)
      throws ExecutionException, InterruptedException {
    int originalCount = 7;
    ArrayList<PooledConnection> originalConnections = new ArrayList<>();
    repeat(() -> {
      PooledConnection original = createConnection();
      originalConnections.add(original);
      instance.addFirst(original, false);
    }, originalCount);

    executor.inParallel(() -> {
      repeat(() -> {
        PooledConnection removed = createConnection();
        instance.addLast(removed, true);
        assertThat(instance.remove(removed)).isTrue();
      }, iterationCount);
    }, parallelCount);
    executor.execute();

    assertThat(instance.getDeque()).containsExactlyInAnyOrderElementsOf(originalConnections);
  }

  private PooledConnection createConnection() {
    PooledConnection result = mock(PooledConnection.class);
    when(result.activate()).thenReturn(true);
    when(result.isActive()).thenReturn(true);
    return result;
  }
}
