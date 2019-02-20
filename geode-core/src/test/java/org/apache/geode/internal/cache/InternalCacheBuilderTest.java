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
import java.util.function.Supplier;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCacheBuilder.InternalCacheConstructor;
import org.apache.geode.internal.cache.InternalCacheBuilder.InternalDistributedSystemConstructor;

public class InternalCacheBuilderTest {

  private static final int ANY_SYSTEM_ID = 12;
  private static final String ANY_MEMBER_NAME = "a-member-name";

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
      (a, b, c, d, e, f) -> {
        throw new AssertionError("throwing cache constructor");
      };

  @Mock
  private Supplier<InternalDistributedSystem> nullSingletonSystemSupplier;

  @Mock
  private Supplier<InternalCache> nullSingletonCacheSupplier;

  @Before
  public void setUp() {
    initMocks(this);

    when(nullSingletonSystemSupplier.get()).thenReturn(null);
    when(nullSingletonCacheSupplier.get()).thenReturn(null);

    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = false;
  }

  @Test // null Properties
  public void create_throwsNullPointerException_ifConfigPropertiesIsNull() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        null, new CacheConfig(),
        nullSingletonSystemSupplier, constructorOf(constructedSystem()),
        nullSingletonCacheSupplier, constructorOf(constructedCache()));

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .create());

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test // null CacheConfig
  public void create_throwsNullPointerException_andCacheConfigIsNull() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), null,
        nullSingletonSystemSupplier, constructorOf(constructedSystem()),
        nullSingletonCacheSupplier, constructorOf(constructedCache()));

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .create());

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test // scenario 1 - no system/no cache - constructs system
  public void create_constructsSystem_withGivenProperties_ifNoSystemExists_andNoCacheExists() {
    InternalCache constructedCache = constructedCache();

    InternalDistributedSystemConstructor systemConstructor = constructorOf(constructedSystem());
    Properties configProperties = new Properties();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        configProperties, new CacheConfig(),
        nullSingletonSystemSupplier, systemConstructor,
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    internalCacheBuilder
        .create();

    verify(systemConstructor).construct(same(configProperties), any());
  }

  @Test // scenario 1 - no system/no cache - returns constructed cache
  public void create_returnsConstructedCache_ifNoSystemExists() {
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        nullSingletonSystemSupplier, constructorOf(constructedSystem()),
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder
        .create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test // scenario 1 - no system/no cache - sets cache on system
  public void create_setsConstructedCache_onConstructedSystem_ifNoSystemExists() {
    InternalDistributedSystem constructedSystem = constructedSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        nullSingletonSystemSupplier, constructorOf(constructedSystem),
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    internalCacheBuilder
        .create();

    verify(constructedSystem).setCache(same(constructedCache));
  }

  @Test // scenario 1 - no system/no cache - sets system on cache
  public void create_setsConstructedSystem_onConstructedCache_ifNoSystemExists_() {
    InternalDistributedSystem constructedSystem = constructedSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        nullSingletonSystemSupplier, constructorOf(constructedSystem),
        nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create();

    verify(cacheConstructor).construct(anyBoolean(), any(), same(constructedSystem), any(),
        anyBoolean(), any());
  }

  @Test // scenario 2 - system/no cache - returns constructed cache
  public void create_returnsConstructedCache_ifSingletonSystemExists_andNoCacheExists() {
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        supplierOf(singletonSystem()), THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder
        .create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test // scenario 2 - system/no cache - sets cache on system
  public void create_setsSingletonSystem_onConstructedCache_ifSingletonSystemExists_andNoCacheExists() {
    InternalDistributedSystem singletonSystem = singletonSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        supplierOf(singletonSystem), THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create();

    verify(singletonSystem).setCache(same(constructedCache));
  }

  @Test // scenario 2 - system/no cache - sets system on cache
  public void create_setsConstructedCache_onSingletonSystem_ifSingletonSystemExists_andNoCacheExists() {
    InternalDistributedSystem singletonSystem = singletonSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        supplierOf(singletonSystem), THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create();

    verify(cacheConstructor).construct(anyBoolean(), any(), same(singletonSystem), any(),
        anyBoolean(), any());
  }

  @Test // scenario 3 - system/closed cache - returns constructed cache
  public void create_returnsConstructedCache_ifSingletonSystemExists_andSingletonCacheIsClosed() {
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        supplierOf(singletonSystem()), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(CLOSED)), constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder
        .create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test // scenario 3 - system/closed cache - sets cache on system
  public void create_setsConstructedCache_onSingletonSystem_ifSingletonSystemExists_andSingletonCacheIsClosed() {
    InternalDistributedSystem singletonSystem = singletonSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        supplierOf(singletonSystem), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(CLOSED)), constructorOf(constructedCache));

    internalCacheBuilder
        .create();

    verify(singletonSystem).setCache(same(constructedCache));
  }

  @Test // scenario 3 - system/closed cache - sets system on cache
  public void create_setsSingletonSystem_onConstructedCache_ifSingletonSystemExists_andSingletonCacheIsClosed() {
    InternalDistributedSystem singletonSystem = singletonSystem();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache());

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        supplierOf(singletonSystem), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(CLOSED)), cacheConstructor);

    internalCacheBuilder
        .create();

    verify(cacheConstructor).construct(anyBoolean(), any(), same(singletonSystem), any(),
        anyBoolean(), any());
  }

  @Test // scenario 4 - system/open cache - notExistingOk throws
  public void create_throwsCacheExistsException_ifSingletonSystemExists_andSingletonCacheIsOpen_butExistingIsNotOk() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        supplierOf(singletonSystem()), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(OPEN)), THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create());

    assertThat(thrown).isInstanceOf(CacheExistsException.class);
  }

  @Test // scenario 5 - system/open cache - incompatible cache throws
  public void create_propagatesCacheConfigException_ifSingletonSystemExists_andSingletonCacheIsOpen_andExistingIsOk_butCacheIsIncompatible() {
    Throwable thrownByCacheConfig = new IllegalStateException("incompatible");

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(thrownByCacheConfig),
        supplierOf(singletonSystem()), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(OPEN)), THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create());

    assertThat(thrown).isSameAs(thrownByCacheConfig);
  }

  @Test // scenario 6 - system/open cache - returns singleton cache
  public void create_returnsSingletonCache_ifSingletonCacheIsOpen() {
    InternalCache singletonCache = singletonCache(OPEN);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        supplierOf(singletonSystem()), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    InternalCache result = internalCacheBuilder
        .create();

    assertThat(result).isSameAs(singletonCache);
  }

  @Test // scenario 7 - given system/no cache - null system throws
  public void createWithSystem_throwsNullPointerException_ifSystemIsNull() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .create(null));

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test // scenario 8 - given system/no cache - returns constructed cache
  public void createWithSystem_returnsConstructedCache_ifNoCacheExists() {
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder
        .create(givenSystem());

    assertThat(result).isSameAs(constructedCache);
  }

  @Test // scenario 8 - given system/no cache - sets cache on system
  public void createWithSystem_setsConstructedCache_onGivenSystem_ifNoCacheExists() {
    InternalDistributedSystem givenSystem = givenSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    internalCacheBuilder
        .create(givenSystem);

    verify(givenSystem).setCache(same(constructedCache));
  }

  @Test // scenario 8 - given system/no cache - sets system on cache
  public void createWithSystem_setsGivenSystem_onConstructedCache_ifNoCacheExists() {
    InternalDistributedSystem givenSystem = givenSystem();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache());

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, cacheConstructor);

    internalCacheBuilder
        .create(givenSystem);

    verify(cacheConstructor).construct(anyBoolean(), any(), same(givenSystem), any(),
        anyBoolean(), any());
  }

  @Test // scenario 9 - given system/closed cache - returns constructed cache
  public void createWithSystem_returnsConstructedCache_ifSingletonCacheIsClosed() {
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(CLOSED)), constructorOf(constructedCache));

    InternalCache result = internalCacheBuilder
        .create(givenSystem());

    assertThat(result).isSameAs(constructedCache);
  }

  @Test // scenario 9 - given system/closed cache - sets cache on system
  public void createWithSystem_setsConstructedCache_onGivenSystem_ifSingletonCacheIsClosed() {
    InternalDistributedSystem givenSystem = givenSystem();
    InternalCache constructedCache = constructedCache();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(CLOSED)), constructorOf(constructedCache));

    internalCacheBuilder
        .create(givenSystem);

    verify(givenSystem).setCache(same(constructedCache));
  }

  @Test // scenario 9 - given system/closed cache - sets system on cache
  public void createWithSystem_setsGivenSystem_onConstructedCache_ifSingletonCacheIsClosed() {
    InternalDistributedSystem givenSystem = givenSystem();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache());

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(CLOSED)), cacheConstructor);

    internalCacheBuilder
        .create(givenSystem);

    verify(cacheConstructor).construct(anyBoolean(), any(), same(givenSystem), any(),
        anyBoolean(), any());
  }

  @Test // scenario 10 - given system/open cache - notExistingOk throws
  public void createWithSystem_throwsCacheExistsException_ifSingletonCacheIsOpen_butExistingIsNotOk() {
    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(OPEN)), THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create(givenSystem()));

    assertThat(thrown).isInstanceOf(CacheExistsException.class);
  }

  @Test // scenario 10 - given system/open cache - does not set cache on system
  public void createWithSystem_doesNotSetSingletonCache_onGivenSystem_ifSingletonCacheIsOpen_butExistingIsNotOk() {
    InternalDistributedSystem givenSystem = givenSystem();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(OPEN)), THROWING_CACHE_CONSTRUCTOR);

    ignoreThrowable(() -> internalCacheBuilder
        .setIsExistingOk(false)
        .create(givenSystem));

    verify(givenSystem, never()).setCache(any());
  }

  @Test // scenario 11 - given system/open cache - incompatible cache throws
  public void createWithSystem_propagatesCacheConfigException_ifSingletonCacheIsOpen_andExistingIsOk_butCacheIsIncompatible() {
    Throwable thrownByCacheConfig = new IllegalStateException("incompatible");

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(thrownByCacheConfig),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(OPEN)), THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem()));

    assertThat(thrown).isSameAs(thrownByCacheConfig);
  }

  @Test // scenario 11 given system/open cache - does not set cache on system
  public void createWithSystem_doesNotSetSingletonCache_onGivenSystem_ifSingletonCacheIsOpen_andExistingIsOk_butCacheIsNotCompatible() {
    InternalDistributedSystem givenSystem = givenSystem();

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), throwingCacheConfig(new IllegalStateException("incompatible")),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache(OPEN)), THROWING_CACHE_CONSTRUCTOR);

    ignoreThrowable(() -> internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem));

    verifyZeroInteractions(givenSystem);
  }

  @Test // scenario 12 - given system/open cache - returns singleton cache
  public void createWithSystem_returnsSingletonCache_ifSingletonCacheIsOpen_andExistingIsOk_andCacheIsCompatible() {
    InternalCache singletonCache = singletonCache(OPEN);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    InternalCache result = internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem());

    assertThat(result).isSameAs(singletonCache);
  }

  @Test // scenario 12 - given system/open cache - sets cache on system
  public void createWithSystem_setsSingletonCache_onGivenSystem_ifSingletonCacheIsOpen_andExistingIsOk_andCacheIsCompatible() {
    InternalDistributedSystem givenSystem = givenSystem();
    InternalCache singletonCache = singletonCache(OPEN);

    InternalCacheBuilder internalCacheBuilder = new InternalCacheBuilder(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    internalCacheBuilder
        .setIsExistingOk(true)
        .create(givenSystem);

    verify(givenSystem).setCache(same(singletonCache));
  }

  private InternalDistributedSystem constructedSystem() {
    return systemWith("constructedSystem", ANY_SYSTEM_ID, ANY_MEMBER_NAME);
  }

  private InternalDistributedSystem givenSystem() {
    return systemWith("givenSystem", ANY_SYSTEM_ID, ANY_MEMBER_NAME);
  }

  private InternalDistributedSystem singletonSystem() {
    return systemWith("singletonSystem", ANY_SYSTEM_ID, ANY_MEMBER_NAME);
  }

  private InternalDistributedSystem systemWith(String mockName, int systemId, String memberName) {
    InternalDistributedSystem system = mock(InternalDistributedSystem.class, mockName);
    DistributionConfig distributionConfig = mock(DistributionConfig.class);
    when(distributionConfig.getDistributedSystemId()).thenReturn(systemId);
    when(system.getConfig()).thenReturn(distributionConfig);
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
    when(constructor.construct(anyBoolean(), any(), any(), any(), anyBoolean(), any()))
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
