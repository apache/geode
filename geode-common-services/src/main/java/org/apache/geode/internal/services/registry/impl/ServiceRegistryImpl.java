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
package org.apache.geode.internal.services.registry.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.apache.geode.services.registry.ServiceRegistry;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

public class ServiceRegistryImpl implements ServiceRegistry {

  private final Map<String, Object> services = new HashMap<>();

  @Override
  public <T> ServiceResult<T> getService(Class<T> clazz) {
    List<Object> matchingServices = services.values().stream()
        .filter(service -> clazz.isAssignableFrom(service.getClass()))
        .collect(Collectors.toList());

    if (matchingServices.isEmpty()) {
      ServiceResult<T> serviceResult = loadAvailableServiceForClass(clazz);
      if (serviceResult.isSuccessful()) {
        addService(serviceResult.getMessage());
      }
      return serviceResult;
    }

    if (matchingServices.size() > 1) {
      List<String> serviceNames =
          matchingServices.stream().map(service -> service.getClass().getName())
              .collect(Collectors.toList());
      String joinedServiceNames = String.join("\n\t", serviceNames);
      return Failure.of(
          "Multiple services for type: " + clazz.getName() + " were found:\n\t"
              + joinedServiceNames);
    }

    return Success.of(clazz.cast(matchingServices.get(0)));
  }

  private <T> ServiceResult<T> loadAvailableServiceForClass(Class<T> clazz) {
    ServiceLoader<T> serviceLoader = ServiceLoader.load(clazz);
    T returnService = null;
    for (T loadedService : serviceLoader) {
      if (returnService != null) {
        return Failure.of("More than 1 service of: " + clazz
            + " was found. Please qualify and register the service required.");
      }
      returnService = loadedService;
    }
    return returnService == null ? Failure.of("No service found for type: " + clazz.getName())
        : Success.of(returnService);
  }

  @Override
  public <T> ServiceResult<T> getService(String serviceName, Class<T> clazz) {
    Object service = services.get(serviceName);
    if (service == null) {
      return Failure.of("No service found for name: " + serviceName);
    }

    if (!clazz.isAssignableFrom(service.getClass())) {
      return Failure.of("Service for name: " + serviceName + " is not of type: " + clazz.getName());
    }

    return Success.of(clazz.cast(service));
  }

  @Override
  public ServiceResult<Boolean> addService(Object service) {
    return addService(service.getClass().getName(), service);
  }

  @Override
  public ServiceResult<Boolean> addService(String serviceName, Object service) {
    if (services.putIfAbsent(serviceName, service) != null) {
      return Failure.of("Service for name: " + serviceName + " already added with type: "
          + service.getClass().getName());
    }

    return Success.SUCCESS_TRUE;
  }

  @Override
  public ServiceResult<Boolean> removeService(String serviceName) {
    services.remove(serviceName);
    return Success.SUCCESS_TRUE;
  }
}
