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
package org.apache.geode.internal.util;

import static java.lang.String.format;
import static org.apache.geode.internal.util.InternalCacheBuilderTestUtil.CacheState.OPEN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.function.Supplier;

import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheBuilder.InternalCacheConstructor;
import org.apache.geode.internal.cache.InternalCacheBuilder.InternalDistributedSystemConstructor;

public class InternalCacheBuilderTestUtil {
  public enum CacheState {
    OPEN(false),
    CLOSED(true);

    private final boolean isClosed;

    CacheState(boolean isClosed) {
      this.isClosed = isClosed;
    }

    boolean isClosed() {
      return isClosed;
    }
  }

  public static InternalCache constructedCache() {
    return cache("constructed", OPEN);
  }

  public static InternalCache cache(CacheState state) {
    return cache("", state);
  }

  public static InternalCache cache(String origin, CacheState state) {
    InternalCache cache = mock(InternalCache.class, format("%s %s cache", state, origin));
    when(cache.isClosed()).thenReturn(state.isClosed());
    doThrow(new CacheExistsException(cache, "cache exists"))
        .when(cache).throwCacheExistsException();
    return cache;
  }

  public static InternalDistributedSystem constructedSystem() {
    return system("constructed system");
  }

  public static InternalDistributedSystem singletonSystem() {
    return system("singleton system");
  }

  public static InternalDistributedSystem systemWithNoCache() {
    return system("system with no cache");
  }

  public static InternalDistributedSystem system(String name) {
    return systemWith(name, 22, "some-member-name", "some-host-name");
  }

  public static InternalDistributedSystem systemWith(InternalCache cache) {
    InternalDistributedSystem system = system("system with " + cache);
    when(system.getCache()).thenReturn(cache);
    return system;
  }

  public static InternalDistributedSystem systemWith(String mockName, int systemId,
      String memberName, String hostName) {
    InternalDistributedSystem system = mock(InternalDistributedSystem.class, mockName);
    DistributionConfig distributionConfig = mock(DistributionConfig.class);
    InternalDistributedMember distributedMember = mock(InternalDistributedMember.class);
    when(distributionConfig.getDistributedSystemId()).thenReturn(systemId);
    when(distributedMember.getHost()).thenReturn(hostName);
    when(system.getConfig()).thenReturn(distributionConfig);
    when(system.getDistributedMember()).thenReturn(distributedMember);
    when(system.getName()).thenReturn(memberName);
    return system;
  }

  public static InternalDistributedSystemConstructor constructorOf(
      InternalDistributedSystem constructedSystem) {
    InternalDistributedSystemConstructor constructor =
        mock(InternalDistributedSystemConstructor.class, "internal distributed system constructor");
    when(constructor.construct(any(), any(), any(), any()))
        .thenReturn(constructedSystem);
    return constructor;
  }

  public static InternalCacheConstructor constructorOf(InternalCache constructedCache) {
    InternalCacheConstructor constructor =
        mock(InternalCacheConstructor.class, "internal cache constructor");
    when(constructor.construct(anyBoolean(), any(), any(), any(), anyBoolean(), any()))
        .thenReturn(constructedCache);
    return constructor;
  }

  public static <T> Supplier<T> supplierOf(T instance) {
    return () -> instance;
  }

  public static CacheConfig throwingCacheConfig(Throwable throwable) {
    CacheConfig cacheConfig = mock(CacheConfig.class);
    doThrow(throwable).when(cacheConfig).validateCacheConfig(any());
    return cacheConfig;
  }

  public static final Supplier<InternalDistributedSystem> THROWING_SYSTEM_SUPPLIER =
      () -> {
        throw new AssertionError("throwing system supplier");
      };

  public static final Supplier<InternalCache> THROWING_CACHE_SUPPLIER =
      () -> {
        throw new AssertionError("throwing cache supplier");
      };

  public static final InternalDistributedSystemConstructor THROWING_SYSTEM_CONSTRUCTOR =
      (configProperties, securityConfig, userMeterRegistries, moduleService) -> {
        throw new AssertionError("throwing system constructor");
      };

  public static final InternalCacheConstructor THROWING_CACHE_CONSTRUCTOR =
      (isClient, poolFactory, internalDistributedSystem, cacheConfig, useAsyncEventListeners,
          typeRegistry) -> {
        throw new AssertionError("throwing cache constructor");
      };
}
