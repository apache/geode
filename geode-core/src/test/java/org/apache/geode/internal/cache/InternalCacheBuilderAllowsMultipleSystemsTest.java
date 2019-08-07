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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.internal.InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS_PROPERTY;
import static org.apache.geode.internal.cache.InternalCacheBuilderAllowsMultipleSystemsTest.CacheState.CLOSED;
import static org.apache.geode.internal.cache.InternalCacheBuilderAllowsMultipleSystemsTest.CacheState.OPEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.assertj.core.api.ThrowableAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.mockito.Mock;

import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCacheBuilder.InternalCacheConstructor;
import org.apache.geode.internal.cache.InternalCacheBuilder.InternalDistributedSystemConstructor;
import org.apache.geode.internal.metrics.CompositeMeterRegistryFactory;

/**
 * Unit tests for {@link InternalCacheBuilder} when
 * {@code InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS} is set to true.
 */
public class InternalCacheBuilderAllowsMultipleSystemsTest {

  private static final Supplier<String> MEMBER_TYPE_SUPPLIER = () -> "a-member-type";
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private static final int ANY_SYSTEM_ID = 12;
  private static final String ANY_MEMBER_NAME = "a-member-name";
  private static final String ANY_HOST_NAME = "a-host-name";

  private static final Supplier<InternalDistributedSystem> THROWING_SYSTEM_SUPPLIER =
      () -> {
        throw new AssertionError("throwing system supplier");
      };
  private static final Supplier<InternalCache> THROWING_CACHE_SUPPLIER =
      () -> {
        throw new AssertionError("throwing cache supplier");
      };

  private static final InternalDistributedSystemConstructor THROWING_SYSTEM_CONSTRUCTOR =
      (a, b) -> {
        throw new AssertionError("throwing system constructor");
      };
  private static final InternalCacheConstructor THROWING_CACHE_CONSTRUCTOR =
      (a, b, c, d, e, f, g, addedMeterSubregistries) -> {
        throw new AssertionError("throwing cache constructor");
      };

  @Mock
  private CompositeMeterRegistryFactory compositeMeterRegistryFactory;

  @Mock
  private Consumer<CompositeMeterRegistry> metricsSessionInitializer;

  @Before
  public void setUp() {
    initMocks(this);

    System.setProperty(ALLOW_MULTIPLE_SYSTEMS_PROPERTY, "true");
  }

  @Test
  public void create_throwsNullPointerException_ifConfigPropertiesIsNull() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        null, new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, constructorOf(constructedSystem()),
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache()));

    Throwable thrown = catchThrowable(internalCacheBuilder::create);

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void create_throwsNullPointerException_andCacheConfigIsNull() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), null,
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, constructorOf(constructedSystem()),
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache()));

    Throwable thrown = catchThrowable(internalCacheBuilder::create);

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void create_constructsSystem_withGivenProperties_ifNoSystemExists() {
    InternalCache constructedCache = constructedCache();

    InternalDistributedSystemConstructor systemConstructor = constructorOf(constructedSystem());
    Properties configProperties = new Properties();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        configProperties, new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, systemConstructor,
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    internalCacheBuilder.create();

    verify(systemConstructor).construct(same(configProperties), any());
  }

  @Test
  public void create_returnsConstructedCache_ifNoSystemExists() {
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, constructorOf(constructedSystem()),
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder.create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void create_setsConstructedCache_onConstructedSystem_ifNoSystemExists() {
    InternalDistributedSystem constructedSystem = constructedSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, constructorOf(constructedSystem),
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    internalCacheBuilder.create();

    verify(constructedSystem).setCache(same(constructedCache));
  }

  @Test
  public void create_setsConstructedSystem_onConstructedCache_ifNoSystemExists() {
    InternalDistributedSystem constructedSystem = constructedSystem();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache());

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, constructorOf(constructedSystem),
        THROWING_CACHE_SUPPLIER, cacheConstructor);

    internalCacheBuilder.create();

    verify(cacheConstructor).construct(anyBoolean(), any(), same(constructedSystem), any(),
        anyBoolean(), any(), any(), any());
  }


  @Test
  public void createWithSystem_throwsNullPointerException_ifSystemIsNull() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder.create(null));

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void createWithSystem_returnsConstructedCache_ifSystemCacheDoesNotExist() {
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder
        .create(givenSystem());

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void createWithSystem_setsConstructedCache_onGivenSystem_ifSystemCacheDoesNotExist() {
    InternalDistributedSystem givenSystem = givenSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    internalCacheBuilder
        .create(givenSystem);

    verify(givenSystem).setCache(same(constructedCache));
  }

  @Test
  public void createWithSystem_setsGivenSystem_onConstructedCache_ifSystemCacheDoesNotExist() {
    InternalDistributedSystem givenSystem = givenSystem();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache());

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, cacheConstructor);

    internalCacheBuilder
        .create(givenSystem);

    verify(cacheConstructor).construct(anyBoolean(), any(), same(givenSystem), any(),
        anyBoolean(), any(), any(), any());
  }

  @Test
  public void createWithSystem_returnsConstructedCache_ifSystemCacheIsClosed() {
    InternalDistributedSystem givenSystem = givenSystemWithCache(CLOSED);
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder
        .create(givenSystem);

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void createWithSystem_setsConstructedCache_onGivenSystem_ifSystemCacheIsClosed() {
    InternalDistributedSystem givenSystem = givenSystemWithCache(CLOSED);
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, constructorOf(constructedCache));

    internalCacheBuilder
        .create(givenSystem);

    verify(givenSystem).setCache(same(constructedCache));
  }

  @Test
  public void createWithSystem_setsGivenSystem_onConstructedCache_ifSystemCacheIsClosed() {
    InternalDistributedSystem givenSystem = givenSystemWithCache(CLOSED);

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache());

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, cacheConstructor);

    internalCacheBuilder
        .create(givenSystem);

    verify(cacheConstructor).construct(anyBoolean(), any(), same(givenSystem), any(),
        anyBoolean(), any(), any(), any());
  }

  @Test
  public void createWithSystem_throwsCacheExistsException_ifSystemCacheIsOpen_butExistingNotOk() {
    InternalDistributedSystem givenSystem = givenSystemWithCache(OPEN);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create(givenSystem));

    assertThat(thrown).isInstanceOf(CacheExistsException.class);
  }

  @Test
  public void createWithSystem_doesNotSetSystemCache_onGivenSystem__ifSystemCacheIsOpen_butExistingNotOk() {
    InternalDistributedSystem givenSystem = givenSystemWithCache(OPEN);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    ignoreThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create(givenSystem));

    verify(givenSystem, never()).setCache(any());
  }

  @Test
  public void createWithSystem_propagatesCacheConfigException_ifSystemCacheIsOpen_andExistingOk_butCacheIsIncompatible() {
    InternalDistributedSystem givenSystem = givenSystemWithCache(OPEN);

    Throwable thrownByCacheConfig = new IllegalStateException("incompatible");

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(thrownByCacheConfig),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem));

    assertThat(thrown).isSameAs(thrownByCacheConfig);
  }

  @Test
  public void createWithSystem_doesNotSetSystemCache_onGivenSystem_ifSystemCacheIsOpen_andExistingOk_butCacheIsNotCompatible() {
    InternalDistributedSystem givenSystem = givenSystemWithCache(OPEN);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(new IllegalStateException("incompatible")),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    ignoreThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem));

    verify(givenSystem, never()).setCache(any());
  }

  @Test
  public void createWithSystem_returnsSystemCache_ifSystemCacheIsOpen_andExistingOk_andCacheIsCompatible() {
    InternalDistributedSystem givenSystem = givenSystem();
    InternalCache systemCache = systemCache(givenSystem, OPEN);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    InternalCache result = internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem);

    assertThat(result).isSameAs(systemCache);
  }

  @Test
  public void createWithSystem_setsSystemCache_onGivenSystem_ifSystemCacheIsOpen_andExistingOk_andCacheIsCompatible() {
    InternalDistributedSystem givenSystem = givenSystem();
    InternalCache systemCache = systemCache(givenSystem, OPEN);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem);

    verify(givenSystem).setCache(same(systemCache));
  }

  private InternalDistributedSystem constructedSystem() {
    return systemWith("constructedSystem", ANY_SYSTEM_ID, ANY_MEMBER_NAME, ANY_HOST_NAME);
  }

  private InternalDistributedSystem givenSystem() {
    return systemWith("givenSystem", ANY_SYSTEM_ID, ANY_MEMBER_NAME, ANY_HOST_NAME);
  }

  private InternalDistributedSystem systemWith(String mockName, int systemId, String memberName,
      String hostName) {
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

  private InternalDistributedSystem givenSystemWithCache(CacheState state) {
    InternalDistributedSystem system =
        systemWith("givenSystemWithCache", ANY_SYSTEM_ID, ANY_MEMBER_NAME, ANY_HOST_NAME);
    systemCache(system, state);
    return system;
  }

  private static InternalCache constructedCache() {
    return cache("constructedCache", OPEN);
  }

  private static InternalCache systemCache(InternalDistributedSystem givenSystem,
      CacheState state) {
    InternalCache cache = cache("systemCache", state);
    when(givenSystem.getCache()).thenReturn(cache);
    return cache;
  }

  private static InternalCache cache(String name, CacheState state) {
    InternalCache cache = mock(InternalCache.class, name);
    when(cache.isClosed()).thenReturn(state.isClosed());
    doThrow(new CacheExistsException(cache, "cache exists"))
        .when(cache).throwCacheExistsException();
    return cache;
  }

  private static InternalDistributedSystemConstructor constructorOf(
      InternalDistributedSystem constructedSystem) {
    InternalDistributedSystemConstructor constructor =
        mock(InternalDistributedSystemConstructor.class, "internal distributed system constructor");
    when(constructor.construct(any(), any())).thenReturn(constructedSystem);
    return constructor;
  }

  private static InternalCacheConstructor constructorOf(InternalCache constructedCache) {
    InternalCacheConstructor constructor =
        mock(InternalCacheConstructor.class, "internal cache constructor");
    when(
        constructor.construct(anyBoolean(), any(), any(), any(), anyBoolean(), any(), any(), any()))
            .thenReturn(constructedCache);
    return constructor;
  }

  private static CacheConfig throwingCacheConfig(Throwable throwable) {
    CacheConfig cacheConfig = mock(CacheConfig.class);
    doThrow(throwable).when(cacheConfig).validateCacheConfig(any());
    return cacheConfig;
  }

  private static void ignoreThrowable(ThrowableAssert.ThrowingCallable shouldRaiseThrowable) {
    try {
      shouldRaiseThrowable.call();
    } catch (Throwable ignored) {
    }
  }

  enum CacheState {
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
}
