/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.services.management.internal;

import static org.apache.geode.services.result.impl.Success.SUCCESS_TRUE;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.apache.logging.log4j.spi.Provider;
import org.apache.logging.log4j.util.ProviderUtil;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.services.management.ComponentManagementService;
import org.apache.geode.services.management.ManagementService;
import org.apache.geode.services.management.impl.ComponentIdentifier;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.services.result.impl.Failure;

/**
 * This {@link ComponentManagementService} is responsible for the creation of a {@link Cache}. This
 * {@link ComponentManagementService} will be called by {@link ManagementService}
 *
 * @see ComponentManagementService
 * @see ManagementService
 * @see Cache
 *
 * @since Geode 1.14
 */
@Experimental
public class CacheComponentManagementService implements ComponentManagementService<Cache> {
  private static final String CACHE = "Cache";
  private Cache cache;
  private Logger logger;

  /**
   * {@inheritDoc}
   */
  @Override
  public ServiceResult<Boolean> init(ModuleService moduleService, Object[] args) {
    if (moduleService == null) {
      return Failure.of("The ModuleService on the ComponentManagementService must not be null");
    }

    reconfigureLog4jToUseGeodeLoggingFormat();
    this.logger = LogManager.getLogger();
    CacheFactory cacheFactory = new CacheFactory((Properties) args[0]);
    cacheFactory.setModuleService(moduleService);
    cache = cacheFactory.create();
    // TODO Udo: This needs to be revisited. This information is either passed in, in a custom
    // format, or stored on the CacheConfig on the InternalDistributedSystem
    return createCacheServer(args);
  }

  private void reconfigureLog4jToUseGeodeLoggingFormat() {
    System.setProperty("log4j.configurationFile", "log4j2.xml");
    final SortedMap<Integer, LoggerContextFactory> factories = new TreeMap<>();
    // note that the following initial call to ProviderUtil may block until a Provider has been
    // installed when
    // running in an OSGi environment
    if (ProviderUtil.hasProviders()) {
      for (final Provider provider : ProviderUtil.getProviders()) {
        final Class<? extends LoggerContextFactory> factoryClass =
            provider.loadLoggerContextFactory();
        if (factoryClass != null) {
          try {
            factories.put(provider.getPriority(), factoryClass.newInstance());
          } catch (final Exception e) {
            e.printStackTrace();
            // LOGGER.error("Unable to create class {} specified in provider URL {}",
            // factoryClass.getName(), provider
            // .getUrl(), e);
          }
        }
      }
      if (factories.size() == 1) {
        LogManager.setFactory(factories.values().stream().findFirst().get());
      }
    }
  }

  private ServiceResult<Boolean> createCacheServer(Object[] args) {
    // TODO Udo: This needs to be revisited. This information is either passed in, in a custom
    // format, or stored on the CacheConfig on the InternalDistributedSystem
    CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.setBindAddress("localhost");
    try {
      cacheServer.start();
    } catch (IOException e) {
      logger.error(e);
      return Failure.of(e);
    }
    return SUCCESS_TRUE;
  }

  /**
   * Returns a boolean indicator if this {@link ComponentManagementService} is able to initialize or
   * manage the component described by the {@link ComponentIdentifier}
   *
   * @param componentIdentifier {@link ComponentIdentifier} used to determine if the Component can
   *        be created by this {@link ComponentManagementService}.
   * @return {@literal true} if this {@link ComponentManagementService} can manage the component
   *         described by the {@link ComponentIdentifier}
   */
  @Override
  public boolean canCreateComponent(ComponentIdentifier componentIdentifier) {
    if (componentIdentifier != null) {
      return CACHE.equals(componentIdentifier.getComponentName());
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Optional<Cache> getInitializedComponent() {
    return Optional.ofNullable(cache);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ServiceResult<Boolean> close(Object[] args) {
    try {
      cache.close();
    } catch (Exception e) {
      logger.warn(e);
      return Failure.of(e);
    }
    if (cache.isClosed()) {
      return SUCCESS_TRUE;
    } else {
      return Failure.of("Cache was not successfully closed");
    }
  }
}
