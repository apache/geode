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
package org.apache.geode.services.module.impl;

import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.services.module.ModuleDescriptor;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ModuleServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

public class ServiceLoaderModuleService implements ModuleService {

  private Logger logger;

  public ServiceLoaderModuleService(Logger logger) {
    this.logger = logger;
  }

  @Override
  public ModuleServiceResult<Boolean> loadModule(ModuleDescriptor moduleDescriptor) {
    return Failure.of("This features is not implemented for a default ModuleService");
  }

  @Override
  public ModuleServiceResult<Boolean> registerModule(ModuleDescriptor moduleDescriptor) {
    return Failure.of("This features is not implemented for a default ModuleService");
  }

  @Override
  public ModuleServiceResult<Boolean> unloadModule(String moduleName) {
    return Failure.of("This features is not implemented for a default ModuleService");
  }

  @Override
  public <T> ModuleServiceResult<Set<T>> loadService(Class<T> service) {
    Set<T> result = new HashSet<>();
    try {
      Iterator<T> iterator = ServiceLoader.load(service).iterator();
      while (iterator.hasNext()) {
        try {
          result.add(iterator.next());
        } catch (Error e) {
          logger.error(e.getMessage());
        }
      }
    } catch (Exception e) {
      return Failure.of(e.toString());
    }
    return Success.of(result);
  }

  @Override
  public ModuleServiceResult<Class<?>> loadClass(String className,
      ModuleDescriptor moduleDescriptor) {
    return Failure.of("This features is not implemented for a default ModuleService");
  }

  @Override
  public ModuleServiceResult<List<Class<?>>> loadClass(String className) {
    try {
      return Success.of(Collections.singletonList(
          this.getClass().getClassLoader().loadClass(className)));
    } catch (ClassNotFoundException e) {
      return Failure.of(e.toString());
    }
  }

  @Override
  public ModuleServiceResult<List<InputStream>> findResourceAsStream(String resourceFile) {
    InputStream inputStream = null;
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    if (contextClassLoader != null) {
      inputStream = contextClassLoader.getResourceAsStream(resourceFile);
    }

    if (inputStream == null) {
      inputStream = getClass().getResourceAsStream(resourceFile);
    }
    if (inputStream == null) {
      inputStream = ClassLoader.getSystemResourceAsStream(resourceFile);
    }

    return inputStream == null
        ? Failure.of(String.format("No resource for path: %s could be found", resourceFile))
        : Success.of(Collections.singletonList(inputStream));
  }

  @Override
  public void setLogger(Logger logger) {
    this.logger = logger;
  }
}
