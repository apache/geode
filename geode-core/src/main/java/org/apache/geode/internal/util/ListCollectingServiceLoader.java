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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.services.classloader.impl.ClassLoaderServiceInstance;
import org.apache.geode.services.result.ServiceResult;

/**
 * Implements {@link CollectingServiceLoader} by returning a {@link List} of all currently loadable
 * implementations of the given service interface.
 */
public class ListCollectingServiceLoader<S> implements CollectingServiceLoader<S> {
  private static final Logger logger = LogManager.getLogger();

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

    private ServiceResult<List<S>> serviceResult;

    @Override
    public void load(Class<S> service) {
      serviceResult = ClassLoaderServiceInstance.getInstance().loadService(service);
    }

    @Override
    public Iterator<S> iterator() throws ServiceConfigurationError {
      if (serviceResult.isSuccessful()) {
        return serviceResult.getMessage().iterator();
      }
      return Collections.emptyIterator();
    }
  }
}
