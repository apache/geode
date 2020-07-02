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

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.client.internal.ClientCacheConnection;
import org.apache.geode.cache.client.internal.ConnectionStats;
import org.apache.geode.cache.client.internal.Endpoint;
import org.apache.geode.cache.client.internal.Op;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
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
        ClientCacheConnection used = instance.useFirst();
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
        ClientCacheConnection used = instance.useFirst(c -> true);
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
        ClientCacheConnection used = instance.useFirst();
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
        ClientCacheConnection used = instance.useFirst();
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
        ClientCacheConnection used = instance.useFirst(c -> true);
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
      ClientCacheConnection nonMatchingConnection = createConnection(1);
      instance.addFirst(nonMatchingConnection, false);
    }, connectionCount);

    executor.inParallel(() -> {
      repeat(() -> {
        ClientCacheConnection used = instance.useFirst(c -> c.getBirthDate() == 0L);
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
    Collection<ClientCacheConnection> originalConnections = new ArrayList<>();
    repeat(() -> {
      ClientCacheConnection original = createConnection();
      originalConnections.add(original);
      instance.addFirst(original, false);
    }, originalCount);

    executor.inParallel(() -> {
      repeat(() -> {
        ClientCacheConnection removed = createConnection();
        instance.addLast(removed, true);
        assertThat(instance.remove(removed)).isTrue();
      }, iterationCount);
    }, parallelCount);
    executor.execute();

    assertThat(instance.getDeque()).containsExactlyInAnyOrderElementsOf(originalConnections);
  }

  private ClientCacheConnection createConnection() {
    return createConnection(0);
  }

  private ClientCacheConnection createConnection(long birthDate) {
    return new ClientCacheConnection() {
      @Override
      public Socket getSocket() {
        return null;
      }

      @Override
      public long getBirthDate() {
        return birthDate;
      }

      @Override
      public void setBirthDate(long ts) {
        // nothing
      }

      @Override
      public ByteBuffer getCommBuffer() {
        return null;
      }

      @Override
      public ConnectionStats getStats() {
        return null;
      }

      @Override
      public boolean isActive() {
        return true;
      }

      @Override
      public void destroy() {
        // nothing
      }

      @Override
      public boolean isDestroyed() {
        return false;
      }

      @Override
      public void close(boolean keepAlive) {
        // nothing
      }

      @Override
      public ServerLocation getServer() {
        return null;
      }

      @Override
      public Endpoint getEndpoint() {
        return null;
      }

      @Override
      public ServerQueueStatus getQueueStatus() {
        return null;
      }

      @Override
      public Object execute(Op op) {
        return null;
      }

      @Override
      public void emergencyClose() {
        // nothing
      }

      @Override
      public short getWanSiteVersion() {
        return 0;
      }

      @Override
      public void setWanSiteVersion(short wanSiteVersion) {
        // nothing
      }

      @Override
      public int getDistributedSystemId() {
        return 0;
      }

      @Override
      public OutputStream getOutputStream() {
        return null;
      }

      @Override
      public InputStream getInputStream() {
        return null;
      }

      @Override
      public void setConnectionID(long id) {
        // nothing
      }

      @Override
      public long getConnectionID() {
        return 0;
      }
    };
  }
}
