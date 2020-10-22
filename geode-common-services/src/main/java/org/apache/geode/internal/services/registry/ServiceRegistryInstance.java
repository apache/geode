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
package org.apache.geode.internal.services.registry;

import org.apache.geode.internal.services.registry.impl.ServiceRegistryImpl;
import org.apache.geode.services.registry.ServiceRegistry;
import org.apache.geode.services.result.ServiceResult;

public class ServiceRegistryInstance {
  private static ServiceRegistry serviceRegistry = new ServiceRegistryImpl();

  public static <T> ServiceResult<T> getService(Class<T> clazz) {
    return serviceRegistry.getService(clazz);
  }

  public static <T> ServiceResult<T> getService(String serviceName, Class<T> clazz) {
    return serviceRegistry.getService(serviceName, clazz);
  }

  public static ServiceResult<Boolean> addService(Object service) {
    return serviceRegistry.addService(service);
  }

  public static ServiceResult<Boolean> addService(String name, Object service) {
    return serviceRegistry.addService(name, service);
  }

  public static ServiceResult<Boolean> removeService(String name) {
    return serviceRegistry.removeService(name);
  }
}
