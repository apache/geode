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
package org.apache.geode.internal.services.classloader.impl;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.deployment.jar.ClassPathLoader;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.classloader.ClassLoaderService;
import org.apache.geode.services.result.ServiceResult;
import org.apache.geode.services.result.impl.Failure;
import org.apache.geode.services.result.impl.Success;

/**
 * Default of {@link ClassLoaderService} using {@link ServiceLoader}.
 *
 * @since Geode 1.14.0
 *
 * @see ClassLoaderService
 * @see ServiceResult
 * @see ServiceLoader
 */
@Experimental
public class DefaultClassLoaderServiceImpl implements ClassLoaderService {

  private Logger logger;

  public DefaultClassLoaderServiceImpl() {
    logger = LogService.getLogger();
  }

  public DefaultClassLoaderServiceImpl(Logger logger) {
    this.logger = logger;
  }

  @Override
  public void setWorkingDirectory(File deployWorkingDir) {
    ClassPathLoader.setLatestToDefault(deployWorkingDir);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <T> ServiceResult<List<T>> loadService(Class<T> service) {
    logger.debug("loadService: " + service);
    if (service == null) {
      return Failure.of("service cannot be null");
    }
    List<T> result = new ArrayList<>();
    Iterator<T> iterator = ServiceLoader.load(service,ClassPathLoader.getLatestAsClassLoader()).iterator();
    while (iterator.hasNext()) {
      try {
        result.add(iterator.next());
      } catch (Error e) {
        logger.error(e);
      }
    }
    if (result.isEmpty()) {
      return Failure.of("No implementations of service: " + service + " found");
    }
    return Success.of(result);
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public ServiceResult<Class<?>> forName(String className) {
    logger.debug("forName: " + className);
    if (className == null) {
      return Failure.of("className cannot be null");
    }
    try {
      return Success.of(ClassPathLoader.getLatest().forName(className));
    } catch (ClassNotFoundException e) {
      logger.warn("Failure!" + e.getMessage());
      return Failure.of(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ServiceResult<InputStream> getResourceAsStream(String resourceFilePath) {
    logger.debug("getResourceAsStream: " + resourceFilePath);
    if (resourceFilePath == null) {
      return Failure.of("resourceFilePath cannot be null");
    }
    InputStream inputStream = ClassPathLoader.getLatest().getResourceAsStream(resourceFilePath);

    if (inputStream == null) {
      ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
      if (contextClassLoader != null) {
        inputStream = contextClassLoader.getResourceAsStream(resourceFilePath);
      }
    }

    if (inputStream == null) {
      inputStream = getClass().getResourceAsStream(resourceFilePath);
    }
    if (inputStream == null) {
      inputStream = ClassLoader.getSystemResourceAsStream(resourceFilePath);
    }

    if (inputStream == null) {
      logger.warn("Failure! getResourceAsStream");
    }
    return inputStream == null
        ? Failure.of(String.format("No resource for path: %s could be found", resourceFilePath))
        : Success.of(inputStream);
  }

  @Override
  public ServiceResult<InputStream> getResourceAsStream(Class<?> clazz, String resourceFilePath) {
    logger.debug("getResourceAsStream: " + resourceFilePath);
    if (resourceFilePath == null) {
      return Failure.of("resourceFilePath cannot be null");
    }
    InputStream inputStream =
        ClassPathLoader.getLatest().getResourceAsStream(clazz, resourceFilePath);
    if (inputStream == null) {
      logger.warn("Failure! getResourceAsStream");
    }
    return inputStream == null
        ? Failure.of(String.format("No resource for path: %s could be found", resourceFilePath))
        : Success.of(inputStream);
  }

  @Override
  public ServiceResult<URL> getResource(String resourceFilePath) {
    logger.debug("getResource: " + resourceFilePath);
    if (resourceFilePath == null) {
      return Failure.of("resourceFilePath cannot be null");
    }
    URL resource = ClassPathLoader.getLatest().getResource(resourceFilePath);
    if (resource == null) {
      logger.warn("Failure! getResource");
    }
    return resource == null
        ? Failure.of("Resource not found for resourcePath: " + resourceFilePath)
        : Success.of(resource);
  }

  @Override
  public ServiceResult<URL> getResource(Class<?> clazz, String resourceFilePath) {
    logger.debug("getResource: " + resourceFilePath);
    if (resourceFilePath == null) {
      return Failure.of("resourceFilePath cannot be null");
    }
    URL resource = ClassPathLoader.getLatest().getResource(clazz, resourceFilePath);
    if (resource == null) {
      logger.warn("Failure! getResource");
    }
    return resource == null
        ? Failure.of("Resource not found for resourcePath: " + resourceFilePath)
        : Success.of(resource);
  }

  @Override
  public ServiceResult<Class<?>> getProxyClass(Class<?>... classes) {
    logger.debug("getProxyClass: " + Arrays.toString(classes));
    if (classes == null) {
      return Failure.of("classes cannot be null");
    }
    try {
      Class<?> proxyClass = ClassPathLoader.getLatest().getProxyClass(classes);
      return proxyClass == null
          ? Failure.of("Proxy class not found for classes: " + Arrays.toString(classes))
          : Success.of(proxyClass);
    } catch (IllegalArgumentException e) {
      logger.warn("Failure!" + e.getMessage());
      return Failure.of(e);
    }
  }

  @Override
  public ClassLoader asClassLoader() {
    return ClassPathLoader.getLatestAsClassLoader();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setLogger(Logger logger) {
    this.logger = logger;
  }
}
