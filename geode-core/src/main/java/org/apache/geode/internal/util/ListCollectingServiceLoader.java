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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Implements {@link CollectingServiceLoader} by returning a {@link List} of all currently loadable
 * implementations of the given service interface.
 */
public class ListCollectingServiceLoader<S> implements CollectingServiceLoader<S> {
  private static final Logger logger = LogService.getLogger();

  private final ServiceLoaderWrapper<S> serviceLoaderWrapper;

  public ListCollectingServiceLoader() {
    this(new DefaultServiceLoader<>());
  }

  @VisibleForTesting
  ListCollectingServiceLoader(ServiceLoaderWrapper<S> serviceLoaderWrapper) {
    this.serviceLoaderWrapper = serviceLoaderWrapper;
  }

  @Override
  public Collection<S> loadServices(Class<S> service) {
    serviceLoaderWrapper.load(service);

    Collection<S> services = new ArrayList<>();
    for (Iterator<S> iterator = serviceLoaderWrapper.iterator(); iterator.hasNext();) {
      try {
        S instance = iterator.next();
        services.add(instance);
      } catch (ServiceConfigurationError serviceConfigurationError) {
        logger.error("Error while loading implementations of {}", service.getName(),
            serviceConfigurationError);
      }
    }

    return services;
  }

  interface ServiceLoaderWrapper<S> {

    void load(Class<S> service);

    Iterator<S> iterator() throws ServiceConfigurationError;
  }

  private static class DefaultServiceLoader<S> implements ServiceLoaderWrapper<S> {

    private ServiceLoader<S> actualServiceLoader;

    @Override
    public void load(Class<S> service) {
      actualServiceLoader = ServiceLoader.load(service);
    }

    @Override
    public Iterator<S> iterator() throws ServiceConfigurationError {
      return actualServiceLoader.iterator();
    }
  }
}
