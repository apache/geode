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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ServerLauncher.Builder;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link ServerLauncher}.
 *
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class ServerLauncherTest {

  @Before
  public void before() throws Exception {
    DistributedSystem.removeSystem(InternalDistributedSystem.getConnectedInstance());
  }

  @Test
  public void canBeMocked() throws Exception {
    ServerLauncher launcher = mock(ServerLauncher.class);
    Cache cache = mock(Cache.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);

    when(launcher.getCache()).thenReturn(cache);
    when(launcher.getCacheConfig()).thenReturn(cacheConfig);
    when(launcher.getId()).thenReturn("ID");
    when(launcher.isWaiting(eq(cache))).thenReturn(true);
    when(launcher.isHelping()).thenReturn(true);

    launcher.startCacheServer(cache);

    verify(launcher, times(1)).startCacheServer(cache);

    assertThat(launcher.getCache()).isSameAs(cache);
    assertThat(launcher.getCacheConfig()).isSameAs(cacheConfig);
    assertThat(launcher.getId()).isSameAs("ID");
    assertThat(launcher.isWaiting(cache)).isTrue();
    assertThat(launcher.isHelping()).isTrue();
  }

  @Test
  public void isServingReturnsTrueWhenCacheHasOneCacheServer() throws Exception {
    Cache cache = mock(Cache.class);
    CacheServer cacheServer = mock(CacheServer.class);
    when(cache.getCacheServers()).thenReturn(Collections.singletonList(cacheServer));

    ServerLauncher launcher = new Builder().build();

    assertThat(launcher.isServing(cache)).isTrue();
  }

  @Test
  public void isServingReturnsFalseWhenCacheHasZeroCacheServers() throws Exception {
    Cache cache = mock(Cache.class);
    when(cache.getCacheServers()).thenReturn(Collections.emptyList());

    ServerLauncher launcher = new Builder().build();

    assertThat(launcher.isServing(cache)).isFalse();
  }

  @Test
  public void reconnectedCacheIsClosed() throws Exception {
    Cache cache = mock(Cache.class, "Cache");
    Cache reconnectedCache = mock(Cache.class, "ReconnectedCache");
    when(cache.isReconnecting()).thenReturn(false).thenReturn(false).thenReturn(true);
    when(cache.getCacheServers()).thenReturn(Collections.emptyList());
    when(cache.getReconnectedCache()).thenReturn(reconnectedCache);

    new Builder().setCache(cache).build().waitOnServer();

    verify(cache, atLeast(3)).isReconnecting();
    verify(cache).getReconnectedCache();
    verify(reconnectedCache).close();
  }

  @Test
  public void isRunningReturnsTrueWhenRunningIsSetTrue() throws Exception {
    ServerLauncher launcher = new Builder().build();

    launcher.running.set(true);

    assertThat(launcher.isRunning()).isTrue();
  }

  @Test
  public void isRunningReturnsFalseWhenRunningIsSetFalse() throws Exception {
    ServerLauncher launcher = new Builder().build();

    launcher.running.set(false);

    assertThat(launcher.isRunning()).isFalse();
  }

  @Test
  public void reconnectingDistributedSystemIsDisconnectedOnStop() throws Exception {
    Cache cache = mock(Cache.class, "Cache");
    DistributedSystem system = mock(DistributedSystem.class, "DistributedSystem");
    Cache reconnectedCache = mock(Cache.class, "ReconnectedCache");
    when(cache.isReconnecting()).thenReturn(true);
    when(cache.getReconnectedCache()).thenReturn(reconnectedCache);
    when(reconnectedCache.isReconnecting()).thenReturn(true);
    when(reconnectedCache.getReconnectedCache()).thenReturn(null);
    when(reconnectedCache.getDistributedSystem()).thenReturn(system);

    ServerLauncher launcher = new Builder().setCache(cache).build();
    launcher.running.set(true);
    launcher.stop();

    verify(cache, times(1)).isReconnecting();
    verify(cache, times(1)).getReconnectedCache();
    verify(cache, times(1)).isReconnecting();
    verify(cache, times(1)).getReconnectedCache();
    verify(reconnectedCache, times(1)).getDistributedSystem();
    verify(system, times(1)).stopReconnecting();
    verify(reconnectedCache, times(1)).close();
  }

  @Test
  public void isWaitingReturnsTrueWhenSystemIsConnected() throws Exception {
    Cache cache = mock(Cache.class, "Cache");
    DistributedSystem system = mock(DistributedSystem.class, "DistributedSystem");
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.isConnected()).thenReturn(true);

    ServerLauncher launcher = new Builder().build();
    launcher.running.set(true);

    assertThat(launcher.isWaiting(cache)).isTrue();
  }

  @Test
  public void isWaitingReturnsFalseWhenSystemIsNotConnected() throws Exception {
    Cache cache = mock(Cache.class, "Cache");
    DistributedSystem system = mock(DistributedSystem.class, "DistributedSystem");
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.isConnected()).thenReturn(false);
    when(cache.isReconnecting()).thenReturn(false);

    ServerLauncher launcher = new Builder().setMemberName("serverOne").build();
    launcher.running.set(true);

    assertThat(launcher.isWaiting(cache)).isFalse();
  }

  @Test
  public void isWaitingReturnsFalseByDefault() throws Exception {
    ServerLauncher launcher = new Builder().build();

    assertThat(launcher.isWaiting(null)).isFalse();
  }

  @Test
  public void isWaitingReturnsFalseWhenNotRunning() throws Exception {
    ServerLauncher launcher = new Builder().build();

    launcher.running.set(false);

    assertThat(launcher.isWaiting(null)).isFalse();
  }

  @Test
  public void isDisableDefaultServerReturnsFalseByDefault() throws Exception {
    ServerLauncher launcher = new Builder().build();

    assertThat(launcher.isDisableDefaultServer()).isFalse();
  }

  @Test
  public void isDefaultServerEnabledForCacheReturnsTrueByDefault() throws Exception {
    Cache cache = mock(Cache.class, "Cache");

    ServerLauncher launcher = new Builder().build();

    assertThat(launcher.isDefaultServerEnabled(cache)).isTrue();
  }

  @Test
  public void isDefaultServerEnabledForNullThrowsNullPointerException() throws Exception {
    ServerLauncher launcher = new Builder().build();

    assertThatThrownBy(() -> launcher.isDefaultServerEnabled(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void isDefaultServerEnabledReturnsFalseWhenCacheServersExist() throws Exception {
    Cache cache = mock(Cache.class, "Cache");
    CacheServer cacheServer = mock(CacheServer.class, "CacheServer");
    when(cache.getCacheServers()).thenReturn(Collections.singletonList(cacheServer));

    ServerLauncher launcher = new Builder().build();

    assertThat(launcher.isDefaultServerEnabled(cache)).isFalse();
  }

  @Test
  public void isDisableDefaultServerReturnsTrueWhenDisabled() throws Exception {
    ServerLauncher launcher = new Builder().setDisableDefaultServer(true).build();

    assertThat(launcher.isDisableDefaultServer()).isTrue();
  }

  @Test
  public void isDefaultServerEnabledReturnsFalseWhenDefaultServerDisabledIsTrueAndNoCacheServersExist()
      throws Exception {
    Cache cache = mock(Cache.class, "Cache");
    when(cache.getCacheServers()).thenReturn(Collections.emptyList());

    ServerLauncher launcher = new Builder().setDisableDefaultServer(true).build();

    assertThat(launcher.isDefaultServerEnabled(cache)).isFalse();
  }

  @Test
  public void isDefaultServerEnabledReturnsFalseWhenDefaultServerDisabledIsTrueAndCacheServersExist()
      throws Exception {
    Cache cache = mock(Cache.class, "Cache");
    CacheServer cacheServer = mock(CacheServer.class, "CacheServer");
    when(cache.getCacheServers()).thenReturn(Collections.singletonList(cacheServer));

    ServerLauncher launcher = new Builder().setDisableDefaultServer(true).build();

    assertThat(launcher.isDefaultServerEnabled(cache)).isFalse();
  }

  @Test
  public void startCacheServerStartsCacheServerWithBuilderValues() throws Exception {
    Cache cache = mock(Cache.class, "Cache");
    CacheServer cacheServer = mock(CacheServer.class, "CacheServer");
    when(cache.getCacheServers()).thenReturn(Collections.emptyList());
    when(cache.addCacheServer()).thenReturn(cacheServer);
    ServerLauncher launcher = new Builder().setServerBindAddress(null).setServerPort(11235).build();

    launcher.startCacheServer(cache);

    verify(cacheServer, times(1)).setBindAddress(null);
    verify(cacheServer, times(1)).setPort(eq(11235));
    verify(cacheServer, times(1)).start();
  }

  @Test
  public void startCacheServerDoesNothingWhenDefaultServerDisabled() throws Exception {
    Cache cache = mock(Cache.class, "Cache");
    CacheServer cacheServer = mock(CacheServer.class, "CacheServer");
    when(cache.getCacheServers()).thenReturn(Collections.emptyList());
    when(cache.addCacheServer()).thenReturn(cacheServer);
    ServerLauncher launcher = new Builder().setDisableDefaultServer(true).build();

    launcher.startCacheServer(cache);

    verify(cacheServer, times(0)).setBindAddress(anyString());
    verify(cacheServer, times(0)).setPort(anyInt());
    verify(cacheServer, times(0)).start();
  }

  @Test
  public void startCacheServerDoesNothingWhenCacheServerAlreadyExists() throws Exception {
    Cache cache = mock(Cache.class, "Cache");
    CacheServer cacheServer1 = mock(CacheServer.class, "CacheServer1");
    CacheServer cacheServer2 = mock(CacheServer.class, "CacheServer2");
    when(cache.getCacheServers()).thenReturn(Collections.singletonList(cacheServer1));
    when(cache.addCacheServer()).thenReturn(cacheServer1);
    ServerLauncher launcher = new Builder().build();

    launcher.startCacheServer(cache);

    verify(cacheServer2, times(0)).setBindAddress(anyString());
    verify(cacheServer2, times(0)).setPort(anyInt());
    verify(cacheServer2, times(0)).start();
  }
}
