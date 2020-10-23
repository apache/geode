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
package org.apache.geode.internal.deployment.jar;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.services.classloader.impl.ClassLoaderServiceInstance;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.deployment.FunctionScanner;
import org.apache.geode.services.result.ServiceResult;

public class DeployJarFunctionUtils {

  private static final Logger logger = LogService.getLogger(DeployJarFunctionUtils.class);
  private static final Pattern PATTERN_SLASH = Pattern.compile("/");

  /**
   * Scan the JAR file and attempt to register any function classes found.
   */

  public static List<Function<?>> registerFunctions(DeployedJar deployedJar)
      throws ClassNotFoundException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Registering functions with DeployedJar: {}", deployedJar);
    }

    File deployedJarFile = deployedJar.getFile();
    BufferedInputStream bufferedInputStream;
    try {

      bufferedInputStream = new BufferedInputStream(new FileInputStream(deployedJarFile));
    } catch (Exception ex) {
      logger.error("Unable to scan jar file for functions");
      return Collections.EMPTY_LIST;
    }

    List<Function<?>> registeredFunctions = new ArrayList<>();

    JarInputStream jarInputStream = null;
    try {
      Collection<String> functionClasses = findFunctionsInThisJar(deployedJar);
      jarInputStream = new JarInputStream(bufferedInputStream);
      JarEntry jarEntry = jarInputStream.getNextJarEntry();

      while (jarEntry != null) {
        if (jarEntry.getName().endsWith(".class")) {
          final String className = PATTERN_SLASH.matcher(jarEntry.getName()).replaceAll("\\.")
              .substring(0, jarEntry.getName().length() - 6);

          if (functionClasses.contains(className)) {
            if (isDebugEnabled) {
              logger.debug("Attempting to load class: {}, from JAR file: {}", jarEntry.getName(),
                  deployedJarFile.getAbsolutePath());
            }
            try {
              ServiceResult<Class<?>> serviceResult =
                  ClassLoaderServiceInstance.getInstance().forName(className);
              if (serviceResult.isSuccessful()) {
                Class<?> clazz = serviceResult.getMessage();
                Collection<Function<?>> functionsToRegister =
                    getFunctionsToRegisterFromClass(clazz, deployedJar);
                for (Function<?> function : functionsToRegister) {
                  FunctionService.registerFunction(function);
                  if (isDebugEnabled) {
                    logger.debug("Registering function class: {}, from JAR file: {}", className,
                        deployedJarFile.getAbsolutePath());
                  }
                  registeredFunctions.add(function);
                }
              } else {
                throw new ClassNotFoundException(
                    String.format("No class found for name: %s because %s", className,
                        serviceResult.getErrorMessage()));
              }
            } catch (ClassNotFoundException | NoClassDefFoundError cnfex) {
              logger.error("Unable to load all classes from JAR file: {}",
                  deployedJarFile.getAbsolutePath(), cnfex);
              throw cnfex;
            }
          } else {
            if (isDebugEnabled) {
              logger.debug("No functions found in class: {}, from JAR file: {}", jarEntry.getName(),
                  deployedJarFile.getAbsolutePath());
            }
          }
        }
        jarEntry = jarInputStream.getNextJarEntry();
      }
    } catch (IOException exception) {
      logger.error("Exception when trying to read class from ByteArrayInputStream", exception);
    } finally {
      if (jarInputStream != null) {
        try {
          jarInputStream.close();
        } catch (IOException exception) {
          logger.error("Exception attempting to close JAR input stream", exception);
        }
      }
    }
    return registeredFunctions;
  }

  private static Collection<String> findFunctionsInThisJar(DeployedJar deployedJar)
      throws IOException {
    return new FunctionScanner().findFunctionsInJar(deployedJar.getFile());
  }

  /**
   * Check to see if the class implements the Function interface. If so, it will be registered with
   * FunctionService. Also, if the functions's class was originally declared in a cache.xml file
   * then any properties specified at that time will be reused when re-registering the function.
   *
   * @param clazz Class to check for implementation of the Function class
   * @return A collection of Objects that implement the Function interface.
   */
  private static Collection<Function<?>> getFunctionsToRegisterFromClass(Class<?> clazz,
      DeployedJar deployedJar) {
    final List<Function<?>> functionsToRegister = new ArrayList<>();

    try {
      if (Function.class.isAssignableFrom(clazz) && !Modifier.isAbstract(clazz.getModifiers())) {
        boolean registerUninitializedFunction = true;
        if (Declarable.class.isAssignableFrom(clazz)) {
          try {
            InternalCache cache = (InternalCache) CacheFactory.getAnyInstance();
            final List<Properties> propertiesList = cache.getDeclarableProperties(clazz.getName());

            if (!propertiesList.isEmpty()) {
              registerUninitializedFunction = false;
              // It's possible that the same function was declared multiple times in cache.xml
              // with different properties. So, register the function using each set of
              // properties.
              for (Properties properties : propertiesList) {
                @SuppressWarnings("unchecked")
                Function<?> function = newFunction((Class<Function>) clazz, true);
                if (function != null) {
                  ((Declarable) function).initialize(cache, properties);
                  ((Declarable) function).init(properties); // for backwards compatibility
                  if (function.getId() != null) {
                    functionsToRegister.add(function);
                  }
                }
              }
            }
          } catch (CacheClosedException ignored) {
            // That's okay, it just means there were no properties to init the function with
          }
        }

        if (registerUninitializedFunction) {
          @SuppressWarnings("unchecked")
          Function<?> function = newFunction((Class<Function>) clazz, false);
          if (function != null && function.getId() != null) {
            functionsToRegister.add(function);
          }
        }
      }
    } catch (Exception ex) {
      logger.error("Attempting to register function from JAR file: {}",
          deployedJar.getFile().getAbsolutePath(), ex);
    }

    return functionsToRegister;
  }

  private static Function<?> newFunction(final Class<Function> clazz,
      final boolean errorOnNoSuchMethod) {
    try {
      final Constructor<Function> constructor = clazz.getConstructor();
      return constructor.newInstance();
    } catch (NoSuchMethodException exception) {
      if (errorOnNoSuchMethod) {
        logger.error("Zero-arg constructor is required, but not found for class: {}",
            clazz.getName(), exception);
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Not registering function because it doesn't have a zero-arg constructor: {}",
              clazz.getName());
        }
      }
    } catch (Exception ex) {
      logger.error("Error when attempting constructor for function for class: {}", clazz.getName(),
          ex);
    }

    return null;
  }
}
