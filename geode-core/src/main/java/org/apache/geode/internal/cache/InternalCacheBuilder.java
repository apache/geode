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

import static java.util.Objects.requireNonNull;
import static org.apache.geode.distributed.internal.InternalDistributedSystem.ALLOW_MULTIPLE_SYSTEMS_PROPERTY;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;

import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.GatewayException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.SecurityConfig;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.metrics.internal.InternalDistributedSystemMetricsService;
import org.apache.geode.metrics.internal.MetricsService;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.services.module.ModuleService;

public class InternalCacheBuilder {
  private static final Logger logger = LogService.getLogger();

  private static final String USE_ASYNC_EVENT_LISTENERS_PROPERTY =
      GEMFIRE_PREFIX + "Cache.ASYNC_EVENT_LISTENERS";

  private static final boolean IS_EXISTING_OK_DEFAULT = true;
  private static final boolean IS_CLIENT_DEFAULT = false;

  private final Properties configProperties;
  private final CacheConfig cacheConfig;
  private final Supplier<InternalDistributedSystem> singletonSystemSupplier;
  private final Supplier<InternalCache> singletonCacheSupplier;
  private final InternalDistributedSystemConstructor internalDistributedSystemConstructor;
  private final InternalCacheConstructor internalCacheConstructor;
  private final MetricsService.Builder metricsSessionBuilder;

  private boolean isExistingOk = IS_EXISTING_OK_DEFAULT;
  private boolean isClient = IS_CLIENT_DEFAULT;

  private ModuleService moduleService;

  /**
   * Setting useAsyncEventListeners to true will invoke event listeners in asynchronously.
   *
   * <p>
   * Default is specified by system property {@code gemfire.Cache.ASYNC_EVENT_LISTENERS}.
   */
  private boolean useAsyncEventListeners = Boolean.getBoolean(USE_ASYNC_EVENT_LISTENERS_PROPERTY);

  private PoolFactory poolFactory;
  private TypeRegistry typeRegistry;

  /**
   * Creates a cache factory with default configuration properties.
   */
  public InternalCacheBuilder(ModuleService moduleService) {
    this(new Properties(), new CacheConfig(), moduleService);
  }

  /**
   * Create a cache factory initialized with the given configuration properties. For a list of valid
   * configuration properties and their meanings see {@link ConfigurationProperties}.
   *
   * @param configProperties the configuration properties to initialize the factory with.
   */
  public InternalCacheBuilder(Properties configProperties, ModuleService moduleService) {
    this(configProperties == null ? new Properties() : configProperties, new CacheConfig(),
        moduleService);
  }

  /**
   * Creates a cache factory with default configuration properties.
   */
  public InternalCacheBuilder(CacheConfig cacheConfig, ModuleService moduleService) {
    this(new Properties(), cacheConfig, moduleService);
  }

  private InternalCacheBuilder(Properties configProperties, CacheConfig cacheConfig,
      ModuleService moduleService) {
    this(configProperties,
        cacheConfig,
        new InternalDistributedSystemMetricsService.Builder(),
        InternalDistributedSystem::getConnectedInstance,
        InternalDistributedSystem::connectInternal,
        GemFireCacheImpl::getInstance,
        (isClient1, poolFactory1, internalDistributedSystem, cacheConfig1, useAsyncEventListeners1,
            typeRegistry1, moduleService1) -> new GemFireCacheImpl(
                isClient1, poolFactory1, internalDistributedSystem, cacheConfig1,
                useAsyncEventListeners1, typeRegistry1, moduleService1),
        moduleService);
  }

  @VisibleForTesting
  InternalCacheBuilder(Properties configProperties,
      CacheConfig cacheConfig,
      MetricsService.Builder metricsSessionBuilder,
      Supplier<InternalDistributedSystem> singletonSystemSupplier,
      InternalDistributedSystemConstructor internalDistributedSystemConstructor,
      Supplier<InternalCache> singletonCacheSupplier,
      InternalCacheConstructor internalCacheConstructor,
      ModuleService moduleService) {
    this.configProperties = configProperties;
    this.cacheConfig = cacheConfig;
    this.singletonSystemSupplier = singletonSystemSupplier;
    this.internalDistributedSystemConstructor = internalDistributedSystemConstructor;
    this.internalCacheConstructor = internalCacheConstructor;
    this.singletonCacheSupplier = singletonCacheSupplier;
    this.metricsSessionBuilder = metricsSessionBuilder;
    this.metricsSessionBuilder.setIsClient(isClient);
    this.moduleService = moduleService;
  }

  /**
   * @see CacheFactory#create()
   *
   * @throws CacheXmlException If a problem occurs while parsing the declarative caching XML file.
   * @throws TimeoutException If a {@link Region#put(Object, Object)} times out while initializing
   *         the cache.
   * @throws CacheWriterException If a {@code CacheWriterException} is thrown while initializing the
   *         cache.
   * @throws GatewayException If a {@code GatewayException} is thrown while initializing the cache.
   * @throws RegionExistsException If the declarative caching XML file describes a region that
   *         already exists (including the root region).
   * @throws IllegalStateException if cache already exists and is not compatible with the new
   *         configuration.
   * @throws AuthenticationFailedException if authentication fails.
   * @throws AuthenticationRequiredException if the distributed system is in secure mode and this
   *         new member is not configured with security credentials.
   */
  public InternalCache create()
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    synchronized (InternalCacheBuilder.class) {
      InternalDistributedSystem internalDistributedSystem = findInternalDistributedSystem()
          .orElseGet(this::createInternalDistributedSystem);
      return create(internalDistributedSystem);
    }
  }

  /**
   * @see CacheFactory#create(DistributedSystem)
   *
   * @throws IllegalArgumentException If {@code system} is not {@link DistributedSystem#isConnected
   *         connected}.
   * @throws CacheExistsException If an open cache already exists.
   * @throws CacheXmlException If a problem occurs while parsing the declarative caching XML file.
   * @throws TimeoutException If a {@link Region#put(Object, Object)} times out while initializing
   *         the cache.
   * @throws CacheWriterException If a {@code CacheWriterException} is thrown while initializing the
   *         cache.
   * @throws GatewayException If a {@code GatewayException} is thrown while initializing the cache.
   * @throws RegionExistsException If the declarative caching XML file describes a region that
   *         already exists (including the root region).
   */
  public InternalCache create(InternalDistributedSystem internalDistributedSystem)
      throws TimeoutException, CacheWriterException, GatewayException, RegionExistsException {
    requireNonNull(internalDistributedSystem, "internalDistributedSystem");
    try {
      synchronized (InternalCacheBuilder.class) {
        synchronized (GemFireCacheImpl.class) {
          InternalCache cache =
              existingCache(internalDistributedSystem::getCache, singletonCacheSupplier);
          if (cache == null) {
            cache =
                internalCacheConstructor.construct(isClient, poolFactory, internalDistributedSystem,
                    cacheConfig, useAsyncEventListeners, typeRegistry, moduleService);

            internalDistributedSystem.setCache(cache);
            cache.initialize();
          } else {
            internalDistributedSystem.setCache(cache);
          }

          return cache;
        }
      }
    } catch (CacheXmlException | IllegalArgumentException e) {
      logger.error(e.getLocalizedMessage());
      throw e;
    } catch (Error | RuntimeException e) {
      logger.error(e);
      throw e;
    }
  }

  /**
   * @see CacheFactory#set(String, String)
   */
  public InternalCacheBuilder set(String name, String value) {
    configProperties.setProperty(name, value);
    return this;
  }

  /**
   * @see CacheFactory#setPdxReadSerialized(boolean)
   */
  public InternalCacheBuilder setPdxReadSerialized(boolean readSerialized) {
    cacheConfig.setPdxReadSerialized(readSerialized);
    return this;
  }

  /**
   * @see CacheFactory#setSecurityManager(SecurityManager)
   */
  public InternalCacheBuilder setSecurityManager(SecurityManager securityManager) {
    cacheConfig.setSecurityManager(securityManager);
    return this;
  }

  /**
   * @see CacheFactory#setPostProcessor(PostProcessor)
   */
  public InternalCacheBuilder setPostProcessor(PostProcessor postProcessor) {
    cacheConfig.setPostProcessor(postProcessor);
    return this;
  }

  /**
   * @see CacheFactory#setPdxSerializer(PdxSerializer)
   */
  public InternalCacheBuilder setPdxSerializer(PdxSerializer serializer) {
    cacheConfig.setPdxSerializer(serializer);
    return this;
  }

  /**
   * @see CacheFactory#setPdxDiskStore(String)
   */
  public InternalCacheBuilder setPdxDiskStore(String diskStoreName) {
    cacheConfig.setPdxDiskStore(diskStoreName);
    return this;
  }

  /**
   * @see CacheFactory#setPdxPersistent(boolean)
   */
  public InternalCacheBuilder setPdxPersistent(boolean isPersistent) {
    cacheConfig.setPdxPersistent(isPersistent);
    return this;
  }

  /**
   * @see CacheFactory#setPdxIgnoreUnreadFields(boolean)
   */
  public InternalCacheBuilder setPdxIgnoreUnreadFields(boolean ignore) {
    cacheConfig.setPdxIgnoreUnreadFields(ignore);
    return this;
  }

  public InternalCacheBuilder setCacheXMLDescription(String cacheXML) {
    if (cacheXML != null) {
      cacheConfig.setCacheXMLDescription(cacheXML);
    }
    return this;
  }

  /**
   * @param isExistingOk default is true.
   */
  public InternalCacheBuilder setIsExistingOk(boolean isExistingOk) {
    this.isExistingOk = isExistingOk;
    return this;
  }

  /**
   * @param isClient default is false.
   */
  public InternalCacheBuilder setIsClient(boolean isClient) {
    this.isClient = isClient;
    metricsSessionBuilder.setIsClient(isClient);
    return this;
  }

  /**
   * @param useAsyncEventListeners default is specified by the system property
   *        {@code gemfire.Cache.ASYNC_EVENT_LISTENERS}.
   */
  public InternalCacheBuilder setUseAsyncEventListeners(boolean useAsyncEventListeners) {
    this.useAsyncEventListeners = useAsyncEventListeners;
    return this;
  }

  /**
   * @param poolFactory default is null.
   */
  public InternalCacheBuilder setPoolFactory(PoolFactory poolFactory) {
    this.poolFactory = poolFactory;
    return this;
  }

  /**
   * @param typeRegistry default is null.
   */
  public InternalCacheBuilder setTypeRegistry(TypeRegistry typeRegistry) {
    this.typeRegistry = typeRegistry;
    return this;
  }

  /**
   * @see CacheFactory#addMeterSubregistry(MeterRegistry)
   */
  public InternalCacheBuilder addMeterSubregistry(MeterRegistry subregistry) {
    requireNonNull(subregistry, "meter registry");
    metricsSessionBuilder.addPersistentMeterRegistry(subregistry);
    return this;
  }

  private Optional<InternalDistributedSystem> findInternalDistributedSystem() {
    InternalDistributedSystem internalDistributedSystem = null;
    if (configProperties.isEmpty() && !allowMultipleSystems()) {
      // any ds will do
      internalDistributedSystem = singletonSystemSupplier.get();
      validateUsabilityOfSecurityCallbacks(internalDistributedSystem, cacheConfig);
    }
    return Optional.ofNullable(internalDistributedSystem);
  }

  private InternalDistributedSystem createInternalDistributedSystem() {
    SecurityConfig securityConfig = new SecurityConfig(
        cacheConfig.getSecurityManager(),
        cacheConfig.getPostProcessor());

    return internalDistributedSystemConstructor
        .construct(configProperties, securityConfig, metricsSessionBuilder,
            moduleService);
  }

  private InternalCache existingCache(Supplier<? extends InternalCache> systemCacheSupplier,
      Supplier<? extends InternalCache> singletonCacheSupplier) {
    InternalCache cache = allowMultipleSystems()
        ? systemCacheSupplier.get()
        : singletonCacheSupplier.get();

    if (validateExistingCache(cache)) {
      return cache;
    }

    return null;
  }

  /**
   * Validates that isExistingOk is true and existing cache is compatible with cacheConfig.
   *
   * if instance exists and cacheConfig is incompatible
   * if instance exists and isExistingOk is false
   */
  private boolean validateExistingCache(InternalCache existingCache) {
    if (existingCache == null || existingCache.isClosed()) {
      return false;
    }

    if (isExistingOk) {
      cacheConfig.validateCacheConfig(existingCache);
    } else {
      existingCache.throwCacheExistsException();
    }

    return true;
  }

  /**
   * if existing DistributedSystem connection cannot use specified SecurityManager or
   * PostProcessor.
   */
  private static void validateUsabilityOfSecurityCallbacks(
      InternalDistributedSystem internalDistributedSystem, CacheConfig cacheConfig)
      throws GemFireSecurityException {
    if (internalDistributedSystem == null) {
      return;
    }
    // pre-existing DistributedSystem already has an incompatible SecurityService in use
    if (cacheConfig.getSecurityManager() != null) {
      throw new GemFireSecurityException(
          "Existing DistributedSystem connection cannot use specified SecurityManager");
    }
    if (cacheConfig.getPostProcessor() != null) {
      throw new GemFireSecurityException(
          "Existing DistributedSystem connection cannot use specified PostProcessor");
    }
  }

  private static boolean allowMultipleSystems() {
    return Boolean.getBoolean(ALLOW_MULTIPLE_SYSTEMS_PROPERTY);
  }

  public InternalCacheBuilder setModuleService(ModuleService moduleService) {
    this.moduleService = moduleService;
    return this;
  }


  @VisibleForTesting
  public interface InternalCacheConstructor {
    InternalCache construct(boolean isClient, PoolFactory poolFactory,
        InternalDistributedSystem internalDistributedSystem, CacheConfig cacheConfig,
        boolean useAsyncEventListeners, TypeRegistry typeRegistry, ModuleService moduleService);
  }

  @VisibleForTesting
  public interface InternalDistributedSystemConstructor {
    InternalDistributedSystem construct(Properties configProperties, SecurityConfig securityConfig,
        MetricsService.Builder metricsSessionBuilder, ModuleService moduleService);
  }
}
