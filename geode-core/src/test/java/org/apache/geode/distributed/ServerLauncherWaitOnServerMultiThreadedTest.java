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
package org.apache.geode.distributed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.umd.cs.mtc.MultithreadedTestCase;
import edu.umd.cs.mtc.TestFramework;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;

/**
 * Multithreaded unit tests for {@link ServerLauncher}. Extracted from {@link ServerLauncherTest}.
 */
public class ServerLauncherWaitOnServerMultiThreadedTest {

  private final AtomicBoolean connectionStateHolder = new AtomicBoolean(true);

  private Cache cache;
  private ServerLauncher serverLauncher;

  @Before
  public void setUp() throws Exception {
    DistributedSystem system = mock(DistributedSystem.class, "DistributedSystem");
    when(system.isConnected()).thenAnswer(invocation -> connectionStateHolder.get());

    cache = mock(Cache.class, "Cache");
    when(cache.getDistributedSystem()).thenReturn(system);
    when(cache.isReconnecting()).thenReturn(false);
    when(cache.getCacheServers()).thenReturn(Collections.emptyList());
  }

  @Test
  public void waitOnServer() {
    runTest(new ServerWaitMultiThreadedTestCase());
  }

  private static void runTest(final MultithreadedTestCase test) {
    try {
      TestFramework.runOnce(test);
    } catch (Throwable t) {
      throw new AssertionError(t);
    }
  }

  /**
   * Implementation of MultithreadedTestCase.
   */
  private class ServerWaitMultiThreadedTestCase extends MultithreadedTestCase {

    @Override
    public void initialize() {
      serverLauncher = new ServerLauncher.Builder().setMemberName("dataMember")
          .setDisableDefaultServer(true).setCache(cache).build();

      assertThat(connectionStateHolder.get()).isTrue();
    }

    public void thread1() {
      assertTick(0);

      Thread.currentThread().setName("GemFire Data Member 'main' Thread");
      serverLauncher.running.set(true);

      assertThat(serverLauncher.isRunning()).isTrue();
      assertThat(serverLauncher.isServing(serverLauncher.getCache())).isFalse();
      assertThat(serverLauncher.isWaiting(serverLauncher.getCache())).isTrue();

      serverLauncher.waitOnServer();

      assertTick(1); // NOTE the tick does not advance when the other Thread terminates
    }

    public void thread2() {
      waitForTick(1);

      Thread.currentThread().setName("GemFire 'shutdown' Thread");

      assertThat(serverLauncher.isRunning()).isTrue();

      connectionStateHolder.set(false);
    }

    @Override
    public void finish() {
      assertThat(serverLauncher.isRunning()).isFalse();
    }
  }
}
