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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;

public class ServerLauncherWaitOnServerTest {
  @Mock
  DistributedSystem system;

  @Mock
  Cache cache;

  private ServerLauncher serverLauncher;

  @Before
  public void setup() {
    initMocks(this);
    serverLauncher = new ServerLauncher.Builder()
        .setMemberName("dataMember")
        .setDisableDefaultServer(true)
        .setCache(cache)
        .build();

    when(cache.getDistributedSystem()).thenReturn(system);
  }

  @Test(timeout = 60_000)
  public void returnsWhenLauncherIsRunningAndSystemIsNotConnected() {
    serverLauncher.running.set(true);
    when(cache.getCacheServers()).thenReturn(emptyList());
    when(cache.isReconnecting()).thenReturn(false);

    when(system.isConnected())
        .thenReturn(true, true, true) // Connected for a while...
        .thenReturn(false); // ... then not

    serverLauncher.waitOnServer();

    // Four times: false, false, false, true
    verify(system, times(4)).isConnected();
  }

  @Test(timeout = 60_000)
  public void returnsWhenLauncherIsRunningAndCacheIsNotReconnecting() {
    serverLauncher.running.set(true);
    when(cache.getCacheServers()).thenReturn(emptyList());
    when(system.isConnected()).thenReturn(false);

    when(cache.isReconnecting())
        .thenReturn(true, true, true) // Reconnecting for a while...
        .thenReturn(false); // ... then not

    serverLauncher.waitOnServer();

    // Four times: true, true, true, false
    verify(cache, times(4)).isReconnecting();
  }

  @Test(timeout = 60_000)
  public void returnsWhenLauncherIsNotRunning() {
    when(cache.getCacheServers()).thenReturn(emptyList());
    when(system.isConnected()).thenReturn(true);
    when(cache.isReconnecting()).thenReturn(false);

    // Running at first...
    serverLauncher.running.set(true);

    // Using isReconnecting() as a test hook to change running state
    // while we're in the while loop.
    when(cache.isReconnecting())
        .thenReturn(false, false, false)
        .thenAnswer(invocation -> {
          serverLauncher.running.set(false); // ... then not running
          return false;
        });

    serverLauncher.waitOnServer();

    assertThat(serverLauncher.isRunning()).isFalse();
  }

  @Test(timeout = 60_000)
  public void returnsImmediatelyIfCacheHasServers() {
    serverLauncher.running.set(true);
    List<CacheServer> servers = singletonList(mock(CacheServer.class));
    when(cache.getCacheServers()).thenReturn(servers);
    when(system.isConnected()).thenReturn(true);
    when(cache.isReconnecting()).thenReturn(false);

    serverLauncher.waitOnServer();
  }
}
