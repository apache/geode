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

import static org.apache.geode.internal.cache.InternalCacheFactoryTest.CacheState.CLOSED;
import static org.apache.geode.internal.cache.InternalCacheFactoryTest.CacheState.OPEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import java.util.Properties;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCacheFactory.InternalCacheConstructor;
import org.apache.geode.internal.cache.InternalCacheFactory.InternalDistributedSystemConstructor;

public class InternalCacheFactoryTest {

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
  }

  @After
  public void tearDown() {
    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = false;
  }

  @Test
  public void create_constructsCache_ifSingletonCacheDoesNotExist() {
    InternalDistributedSystem constructedSystem = system();
    InternalCache constructedCache = constructedCache();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);
    CacheConfig cacheConfig = new CacheConfig();

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), cacheConfig,
        nullSingletonSystemSupplier, constructorOf(constructedSystem),
        nullSingletonCacheSupplier, cacheConstructor);

    InternalCache result = internalCacheFactory.create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void create_setsConstructedSystem_onConstructedCache_ifSingletonCacheDoesNotExist() {
    InternalDistributedSystem constructedSystem = system();
    InternalCache constructedCache = constructedCache();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        nullSingletonSystemSupplier, constructorOf(constructedSystem),
        nullSingletonCacheSupplier, cacheConstructor);

    internalCacheFactory.create();

    verify(cacheConstructor).construct(anyBoolean(), any(), same(constructedSystem), any(),
        anyBoolean(), any());
  }

  @Test
  public void create_constructsCache_ifSingletonCacheIsClosed() {
    InternalDistributedSystem constructedSystem = system();
    InternalCache singletonCache = singletonCache(CLOSED);
    InternalCache constructedCache = constructedCache();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        nullSingletonSystemSupplier, constructorOf(constructedSystem),
        supplierOf(singletonCache), cacheConstructor);

    InternalCache result = internalCacheFactory.create();

    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void create_setsConstructedSystem_onConstructedCache_ifSingletonCacheIsClosed() {
    InternalDistributedSystem constructedSystem = system();
    InternalCache singletonCache = singletonCache(CLOSED);
    InternalCache constructedCache = constructedCache();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        nullSingletonSystemSupplier, constructorOf(constructedSystem),
        supplierOf(singletonCache), cacheConstructor);

    internalCacheFactory.create();

    verify(cacheConstructor).construct(anyBoolean(), any(), same(constructedSystem), any(),
        anyBoolean(), any());
  }

  @Test
  public void create_constructsSystem_withGivenProperties_ifSingletonSystemDoesNotExist() {
    InternalDistributedSystem constructedSystem = system();
    InternalCache constructedCache = constructedCache();

    InternalDistributedSystemConstructor systemConstructor = constructorOf(constructedSystem);
    Properties configProperties = new Properties();

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        configProperties, new CacheConfig(),
        nullSingletonSystemSupplier, systemConstructor,
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    internalCacheFactory.create();

    verify(systemConstructor).construct(same(configProperties), any());
  }

  @Test
  public void create_setsConstructedCache_onConstructedSystem_ifSingletonSystemDoesNotExist() {
    InternalDistributedSystem constructedSystem = system();
    InternalCache constructedCache = constructedCache();

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        nullSingletonSystemSupplier, constructorOf(constructedSystem),
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    internalCacheFactory.create();

    verify(constructedSystem).setCache(same(constructedCache));
  }

  @Test
  public void create_setsConstructedCache_onSingletonSystem_ifSingletonSystemExists() {
    InternalDistributedSystem singletonSystem = system();
    InternalCache constructedCache = constructedCache();

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        supplierOf(singletonSystem), THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, constructorOf(constructedCache));

    internalCacheFactory.create();

    verify(singletonSystem).setCache(same(constructedCache));
  }

  // continue here
  @Test
  public void create_returnsSingletonCacheIsOpen() {
    InternalDistributedSystem singletonSystem = system();
    InternalCache singletonCache = singletonCache(OPEN);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        supplierOf(singletonSystem), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    InternalCache result = internalCacheFactory.create();

    verify(singletonSystem).setCache(same(singletonCache));
    assertThat(result).isSameAs(singletonCache);
  }

  /**
   * Characterization test: internalDistributedSystem.setCache is re-invoked even if singletonSystem
   * and singletonCache already exist (and singletonSystem probably already has the reference)
   */
  @Test
  public void create_setsSingletonCacheOnSystem_ifSingletonCacheIsOpen() {
    InternalDistributedSystem singletonSystem = system();
    InternalCache singletonCache = singletonCache(OPEN);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        supplierOf(singletonSystem), THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    InternalCache result = internalCacheFactory.create();

    verify(singletonSystem).setCache(same(singletonCache));
    assertThat(result).isSameAs(singletonCache);
  }

  @Test
  public void create_usesSystemCreatedBySystemFactory_ifSystemSupplierReturnsNull() {
    InternalDistributedSystem constructedSystem = system();
    InternalCache constructedCache = constructedCache();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        nullSingletonSystemSupplier, constructorOf(constructedSystem),
        nullSingletonCacheSupplier, cacheConstructor);

    InternalCache result = internalCacheFactory.create();

    verify(nullSingletonSystemSupplier).get();
    verify(cacheConstructor).construct(anyBoolean(), any(), same(constructedSystem), any(),
        anyBoolean(), any());
    verify(constructedSystem).setCache(same(constructedCache));
    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void createWithSystem_passesGivenSystemToGemFireCacheImplFactory() {
    InternalDistributedSystem givenSystem = system();
    InternalCache constructedCache = constructedCache();

    InternalCacheConstructor cacheConstructor = constructorOf(constructedCache);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        nullSingletonCacheSupplier, cacheConstructor);

    InternalCache result = internalCacheFactory.create(givenSystem);

    verify(cacheConstructor).construct(anyBoolean(), any(), same(givenSystem), any(), anyBoolean(),
        any());
    assertThat(result).isSameAs(constructedCache);
  }

  @Test
  public void createWithSystem_throwsIfSystemIsNull() {
    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheFactory.create(null));

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void createWithSystem_usesSingletonCache_ifExistingOk_andDoesNotAllowMultipleSystems_andSingletonCacheIsCompatibleWithCacheConfig() {
    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = false;
    InternalDistributedSystem givenSystem = system();
    InternalCache singletonCache = singletonCache(OPEN);

    CacheConfig compatibleCacheConfig = compatibleCacheConfig(singletonCache);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), compatibleCacheConfig,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    InternalCache result = internalCacheFactory
        .setIsExistingOk(true)
        .create(givenSystem);

    verify(compatibleCacheConfig).validateCacheConfig(singletonCache);
    verify(givenSystem).setCache(same(result));
    assertThat(result).isSameAs(singletonCache);
  }

  @Test
  public void createWithSystem_throws_ifExistingOk_andDoesNotAllowMultipleSystems_butSingletonCacheIsNotCompatibleWithCacheConfig() {
    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = false;
    InternalDistributedSystem givenSystem = system();
    InternalCache singletonCache = singletonCache(OPEN);

    CacheConfig incompatibleCacheConfig = incompatibleCacheConfig(singletonCache);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), incompatibleCacheConfig,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheFactory
        .setIsExistingOk(true)
        .create(givenSystem));

    assertThat(thrown).isInstanceOf(IllegalStateException.class);
    verify(incompatibleCacheConfig).validateCacheConfig(singletonCache);
    verifyZeroInteractions(givenSystem);
  }

  @Test
  public void createWithSystem_usesCacheFromGivenSystem_ifExistingOk_andAllowsMultipleSystems_andCacheFromGivenSystemIsCompatibleWithCacheConfig() {
    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = true;
    InternalDistributedSystem givenSystem = system();

    InternalCache systemCache = systemCache(givenSystem, OPEN);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    InternalCache result = internalCacheFactory
        .setIsExistingOk(true)
        .create(givenSystem);

    assertThat(result).isSameAs(systemCache);
  }

  @Test
  public void createWithSystem_throws_ifNotExistingOk_butSingletonCacheIsOpen() {
    InternalDistributedSystem givenSystem = system();
    InternalCache singletonCache = singletonCache(OPEN);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(singletonCache), THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheFactory
        .setIsExistingOk(false)
        .create(givenSystem));

    assertThat(thrown).isInstanceOf(CacheExistsException.class);
  }

  @Test
  public void createWithSystem_throws_ifNotExistingOk_andAllowsMultipleSystems_butGivenSystemHasOpenCache() {
    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = true;
    InternalDistributedSystem givenSystem = system();
    InternalCache systemCache = systemCache(givenSystem, OPEN);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), new CacheConfig(),
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        supplierOf(systemCache), THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheFactory
        .setIsExistingOk(false)
        .create(givenSystem));

    assertThat(thrown).isInstanceOf(CacheExistsException.class);
  }

  @Test
  public void createWithSystem_throws_ifExistingOk_andAllowsMultipleSystems_butGivenSystemHasOpenCacheThatIsIncompatibleWithCacheConfig() {
    InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS = true;
    InternalDistributedSystem givenSystem = system();
    InternalCache systemCache = systemCache(givenSystem, OPEN);

    IllegalStateException illegalStateException = new IllegalStateException("incompatible");
    CacheConfig incompatibleCacheConfig =
        incompatibleCacheConfig(systemCache, illegalStateException);

    InternalCacheFactory internalCacheFactory = new InternalCacheFactory(
        new Properties(), incompatibleCacheConfig,
        THROWING_SYSTEM_SUPPLIER, THROWING_SYSTEM_CONSTRUCTOR,
        THROWING_CACHE_SUPPLIER, THROWING_CACHE_CONSTRUCTOR);

    Throwable thrown = catchThrowable(() -> internalCacheFactory
        .setIsExistingOk(true)
        .create(givenSystem));

    assertThat(thrown).isSameAs(illegalStateException);
  }

  private InternalDistributedSystem system() {
    return systemWith(ANY_SYSTEM_ID, ANY_MEMBER_NAME);
  }

  private InternalDistributedSystem systemWith(int systemId, String memberName) {
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
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
    when(constructor.construct(anyBoolean(), any(), any(), any(), anyBoolean(), any()))
        .thenReturn(constructedCache);
    return constructor;
  }

  private static <T> Supplier<T> supplierOf(T instance) {
    return () -> instance;
  }

  private static CacheConfig compatibleCacheConfig(InternalCache cache) {
    CacheConfig cacheConfig = mock(CacheConfig.class);
    // If the cache is compatible, validateCacheConfig() does not throw
    doNothing().when(cacheConfig).validateCacheConfig(same(cache));
    return cacheConfig;
  }

  private static CacheConfig incompatibleCacheConfig(InternalCache cache) {
    return incompatibleCacheConfig(cache, new IllegalStateException("incompatible cache config"));
  }

  private static CacheConfig incompatibleCacheConfig(InternalCache cache,
      IllegalStateException exception) {
    CacheConfig cacheConfig = mock(CacheConfig.class);
    // If the cache is incompatible, validateCacheConfig() throws IllegalStateException
    doThrow(exception).when(cacheConfig).validateCacheConfig(same(cache));
    return cacheConfig;
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
