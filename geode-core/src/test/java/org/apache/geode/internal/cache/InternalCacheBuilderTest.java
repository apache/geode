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

import static org.apache.geode.internal.cache.InternalCacheBuilderTest.CacheState.CLOSED;
import static org.apache.geode.internal.cache.InternalCacheBuilderTest.CacheState.OPEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCacheBuilder.InternalCacheConstructor;
import org.apache.geode.internal.cache.InternalCacheBuilder.InternalDistributedSystemConstructor;
import org.apache.geode.internal.metrics.CompositeMeterRegistryFactory;

/**
 * Unit tests for {@link InternalCacheBuilder}.
 */
public class InternalCacheBuilderTest {

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
  private Supplier<InternalDistributedSystem> nullSingletonSystemSupplier;

  @Mock
  private Supplier<InternalCache> nullSingletonCacheSupplier;

  @Mock
  private CompositeMeterRegistryFactory compositeMeterRegistryFactory;

  @Mock
  private Consumer<CompositeMeterRegistry> metricsSessionInitializer;

  @Before
  public void setUp() {
    initMocks(this);

    when(nullSingletonSystemSupplier.get()).thenReturn(null);
    when(nullSingletonCacheSupplier.get()).thenReturn(null);
  }

  @Test
  public void create_throwsNullPointerException_ifConfigPropertiesIsNull() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        null, new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        nullSingletonSystemSupplier, constructorOf(constructedSystem()),
        nullSingletonCacheSupplier, constructorOf(constructedCache()));

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .create());

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void create_throwsNullPointerException_andCacheConfigIsNull() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), null,
        compositeMeterRegistryFactory, metricsSessionInitializer,
        nullSingletonSystemSupplier, constructorOf(constructedSystem()),
        nullSingletonCacheSupplier, constructorOf(constructedCache()));

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .create());

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void create_constructsSystem_withGivenProperties_ifNoSystemExists_andNoCacheExists() {
    InternalCache constructedCache = constructedCache();

    InternalDistributedSystemConstructor systemConstructor = constructorOf(constructedSystem());
    Properties configProperties = new Properties();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        configProperties, new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        nullSingletonSystemSupplier, systemConstructor,
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    internalCacheBuilder
        .create();

    verify(systemConstructor).construct(same(configProperties), any());
  }

  @Test
  public void create_constructsCompositeMeterRegistry_ifNoCacheExists() {
    int theSystemId = 21;
    String theMemberName = "theMemberName";
    String theHostName = "theHostName";
    boolean isClient = false;
    InternalDistributedSystem theConstructedSystem =
        systemWith("theConstructedSystem", theSystemId, theMemberName, theHostName);
    InternalCache constructedCache = constructedCache();

    CompositeMeterRegistryFactory theCompositeMeterRegistryFactory =
        mock(CompositeMeterRegistryFactory.class);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        theCompositeMeterRegistryFactory, metricsSessionInitializer,
        nullSingletonSystemSupplier, constructorOf(theConstructedSystem),
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    internalCacheBuilder
        .create();

    verify(theCompositeMeterRegistryFactory)
        .create(eq(theSystemId), eq(theMemberName), eq(theHostName), eq(isClient));
  }

  @Test
  public void create_setsConstructedCompositeMeterRegistry_onTheConstructedCache_ifNoCacheExists() {
    InternalCache constructedCache = constructedCache();
    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);

    CompositeMeterRegistry theCompositeMeterRegistry = new CompositeMeterRegistry();
    CompositeMeterRegistryFactory theCompositeMeterRegistryFactory =
        mock(CompositeMeterRegistryFactory.class);
    when(theCompositeMeterRegistryFactory.create(anyInt(), any(), any(), anyBoolean()))
        .thenReturn(theCompositeMeterRegistry);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        theCompositeMeterRegistryFactory, metricsSessionInitializer,
        nullSingletonSystemSupplier, constructorOf(constructedSystem()),
        nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create();

    verify(cacheConstructor).construct(anyBoolean(), any(), any(), any(),
        anyBoolean(), any(), same(theCompositeMeterRegistry), any());
  }

  @Test
  public void create_returnsConstructedCache_ifNoSystemExists() {
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        nullSingletonSystemSupplier, constructorOf(constructedSystem()),
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder
        .create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void create_setsConstructedCache_onConstructedSystem_ifNoSystemExists() {
    InternalDistributedSystem constructedSystem = constructedSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        nullSingletonSystemSupplier, constructorOf(constructedSystem),
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    internalCacheBuilder
        .create();

    verify(constructedSystem).setCache(same(constructedCache));
  }

  @Test
  public void create_setsConstructedSystem_onConstructedCache_ifNoSystemExists_() {
    InternalDistributedSystem constructedSystem = constructedSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        nullSingletonSystemSupplier, constructorOf(constructedSystem),
        nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create();

    verify(cacheConstructor).construct(anyBoolean(), any(), same(constructedSystem), any(),
        anyBoolean(), any(), any(), any());
  }

  @Test
  public void create_returnsConstructedCache_ifSingletonSystemExists_andNoCacheExists() {
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        supplierOf(singletonSystem()), THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder
        .create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void create_setsSingletonSystem_onConstructedCache_ifSingletonSystemExists_andNoCacheExists() {
    InternalDistributedSystem singletonSystem = singletonSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        supplierOf(singletonSystem), THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create();

    verify(singletonSystem).setCache(same(constructedCache));
  }

  @Test
  public void create_setsConstructedCache_onSingletonSystem_ifSingletonSystemExists_andNoCacheExists() {
    InternalDistributedSystem singletonSystem = singletonSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        supplierOf(singletonSystem), THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create();

    verify(cacheConstructor).construct(anyBoolean(), any(), same(singletonSystem), any(),
        anyBoolean(), any(), any(), any());
  }

  @Test
  public void create_returnsConstructedCache_ifSingletonSystemExists_andSingletonCacheIsClosed() {
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        supplierOf(singletonSystem()), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(CLOSED)), constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder
        .create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void create_setsConstructedCache_onSingletonSystem_ifSingletonSystemExists_andSingletonCacheIsClosed() {
    InternalDistributedSystem singletonSystem = singletonSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        supplierOf(singletonSystem), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(CLOSED)), constructorOf(constructedCache));

    internalCacheBuilder
        .create();

    verify(singletonSystem).setCache(same(constructedCache));
  }

  @Test
  public void create_setsSingletonSystem_onConstructedCache_ifSingletonSystemExists_andSingletonCacheIsClosed() {
    InternalDistributedSystem singletonSystem = singletonSystem();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache());

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        supplierOf(singletonSystem), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(CLOSED)), cacheConstructor);

    internalCacheBuilder
        .create();

    verify(cacheConstructor).construct(anyBoolean(), any(), same(singletonSystem), any(),
        anyBoolean(), any(), any(), any());
  }

  @Test
  public void create_throwsCacheExistsException_ifSingletonSystemExists_andSingletonCacheIsOpen_butExistingIsNotOk() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        supplierOf(singletonSystem()), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(OPEN)), THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create());

    assertThat(thrown).isInstanceOf(CacheExistsException.class);
  }

  @Test
  public void create_propagatesCacheConfigException_ifSingletonSystemExists_andSingletonCacheIsOpen_andExistingIsOk_butCacheIsIncompatible() {
    Throwable thrownByCacheConfig = new IllegalStateException("incompatible");

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(thrownByCacheConfig),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        supplierOf(singletonSystem()), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(OPEN)), THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create());

    assertThat(thrown).isSameAs(thrownByCacheConfig);
  }

  @Test
  public void create_returnsSingletonCache_ifSingletonCacheIsOpen() {
    InternalCache singletonCache = singletonCache(OPEN);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        supplierOf(singletonSystem()), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    InternalCache result = internalCacheBuilder
        .create();

    assertThat(result).isSameAs(singletonCache);
  }

  @Test
  public void createWithSystem_throwsNullPointerException_ifSystemIsNull() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .create(null));

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void createWithSystem_returnsConstructedCache_ifNoCacheExists() {
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder
        .create(givenSystem());

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void createWithSystem_setsConstructedCache_onGivenSystem_ifNoCacheExists() {
    InternalDistributedSystem givenSystem = givenSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    internalCacheBuilder
        .create(givenSystem);

    verify(givenSystem).setCache(same(constructedCache));
  }

  @Test
  public void createWithSystem_setsGivenSystem_onConstructedCache_ifNoCacheExists() {
    InternalDistributedSystem givenSystem = givenSystem();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache());

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create(givenSystem);

    verify(cacheConstructor).construct(anyBoolean(), any(), same(givenSystem), any(),
        anyBoolean(), any(), any(), any());
  }

  @Test
  public void createWithSystem_returnsConstructedCache_ifSingletonCacheIsClosed() {
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(CLOSED)), constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder
        .create(givenSystem());

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void createWithSystem_setsConstructedCache_onGivenSystem_ifSingletonCacheIsClosed() {
    InternalDistributedSystem givenSystem = givenSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(CLOSED)), constructorOf(constructedCache));

    internalCacheBuilder
        .create(givenSystem);

    verify(givenSystem).setCache(same(constructedCache));
  }

  @Test
  public void createWithSystem_setsGivenSystem_onConstructedCache_ifSingletonCacheIsClosed() {
    InternalDistributedSystem givenSystem = givenSystem();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache());

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(CLOSED)), cacheConstructor);

    internalCacheBuilder
        .create(givenSystem);

    verify(cacheConstructor).construct(anyBoolean(), any(), same(givenSystem), any(),
        anyBoolean(), any(), any(), any());
  }

  @Test
  public void createWithSystem_throwsCacheExistsException_ifSingletonCacheIsOpen_butExistingIsNotOk() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(OPEN)), THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create(givenSystem()));

    assertThat(thrown).isInstanceOf(CacheExistsException.class);
  }

  @Test
  public void createWithSystem_doesNotSetSingletonCache_onGivenSystem_ifSingletonCacheIsOpen_butExistingIsNotOk() {
    InternalDistributedSystem givenSystem = givenSystem();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(OPEN)), THROWING_CACHE_CONSTRUCTOR);

    ignoreThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create(givenSystem));

    verify(givenSystem, never()).setCache(any());
  }

  @Test
  public void createWithSystem_propagatesCacheConfigException_ifSingletonCacheIsOpen_andExistingIsOk_butCacheIsIncompatible() {
    Throwable thrownByCacheConfig = new IllegalStateException("incompatible");

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(thrownByCacheConfig),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(OPEN)), THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem()));

    assertThat(thrown).isSameAs(thrownByCacheConfig);
  }

  @Test
  public void createWithSystem_doesNotSetSingletonCache_onGivenSystem_ifSingletonCacheIsOpen_andExistingIsOk_butCacheIsNotCompatible() {
    InternalDistributedSystem givenSystem = givenSystem();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(new IllegalStateException("incompatible")),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(OPEN)), THROWING_CACHE_CONSTRUCTOR);

    ignoreThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem));

    verifyZeroInteractions(givenSystem);
  }

  @Test
  public void createWithSystem_returnsSingletonCache_ifSingletonCacheIsOpen_andExistingIsOk_andCacheIsCompatible() {
    InternalCache singletonCache = singletonCache(OPEN);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    InternalCache result = internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem());

    assertThat(result).isSameAs(singletonCache);
  }

  @Test
  public void createWithSystem_setsSingletonCache_onGivenSystem_ifSingletonCacheIsOpen_andExistingIsOk_andCacheIsCompatible() {
    InternalDistributedSystem givenSystem = givenSystem();
    InternalCache singletonCache = singletonCache(OPEN);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem);

    verify(givenSystem).setCache(same(singletonCache));
  }

  @Test
  public void addMeterRegistry_createsCache_withGivenMeterRegistry() {
    MeterRegistry theMeterRegistry = new SimpleMeterRegistry();

    CompositeMeterRegistry theCompositeMeterRegistry = new CompositeMeterRegistry();
    when(compositeMeterRegistryFactory.create(anyInt(), any(), any(), anyBoolean()))
        .thenReturn(theCompositeMeterRegistry);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        compositeMeterRegistryFactory, metricsSessionInitializer,
        nullSingletonSystemSupplier, constructorOf(constructedSystem()),
        nullSingletonCacheSupplier, constructorOf(constructedCache()));

    internalCacheBuilder
        .addMeterSubregistry(theMeterRegistry)
        .create();

    assertThat(theCompositeMeterRegistry.getRegistries()).containsExactly(theMeterRegistry);
  }

  private InternalDistributedSystem constructedSystem() {
    return systemWith("constructedSystem", ANY_SYSTEM_ID, ANY_MEMBER_NAME, ANY_HOST_NAME);
  }

  private InternalDistributedSystem givenSystem() {
    return systemWith("givenSystem", ANY_SYSTEM_ID, ANY_MEMBER_NAME, ANY_HOST_NAME);
  }

  private InternalDistributedSystem singletonSystem() {
    return systemWith("singletonSystem", ANY_SYSTEM_ID, ANY_MEMBER_NAME, ANY_HOST_NAME);
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

  private static InternalCache constructedCache() {
    return cache("constructedCache", OPEN);
  }

  private static InternalCache singletonCache(CacheState state) {
    return cache("singletonCache", state);
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

  private static <T> Supplier<T> supplierOf(T instance) {
    return () -> instance;
  }

  private static CacheConfig throwingCacheConfig(Throwable throwable) {
    CacheConfig cacheConfig = mock(CacheConfig.class);
    doThrow(throwable).when(cacheConfig).validateCacheConfig(any());
    return cacheConfig;
  }

  private static void ignoreThrowable(ThrowingCallable shouldRaiseThrowable) {
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
