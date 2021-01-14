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
package org.apache.geode.cache.internal.execute;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.classloader.internal.ClassPathLoader;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.deployment.FunctionScanner;
import org.apache.geode.management.internal.utils.JarFileUtils;

/**
 * This class is used to register {@link Function}s from deployed jar files. It provides convenience
 * over working with the {@link FunctionService} directly.
 */
public class FunctionToFileTracker {

  private static final Logger logger = LogService.getLogger();

  private final Map<String, List<Function<?>>> deploymentToFunctionsMap = new ConcurrentHashMap<>();

  /**
   * Scan the JAR file and attempt to register any function classes found.
   *
   * @param jarFile The {@link File} that contains {@link Function} that are to be registered
   */
  public synchronized void registerFunctionsFromFile(File jarFile)
      throws ClassNotFoundException, IOException {
    logger.debug("Registering functions for: {}", jarFile.getName());

    List<Function<?>> registeredFunctions = new LinkedList<>();
    try {
      Collection<String> functionClasses = findFunctionsInThisJar(jarFile);
      String filePath = jarFile.getAbsolutePath();
      Collection<Function<?>> functions = new LinkedList<>();
      for (String functionClass : functionClasses) {
        logger.debug("Attempting to load class: {}, from JAR file: {}", functionClass,
            filePath);
        try {
          functions.addAll(loadFunctionFromClassName(functionClass));
        } catch (ClassNotFoundException | NoClassDefFoundError cnfex) {
          logger.error("Unable to load all classes from JAR file: {}",
              filePath, cnfex);
          throw cnfex;
        }
      }
      registeredFunctions.addAll(registerFunction(filePath, functions));
    } catch (IOException ioex) {
      logger.error("Exception when trying to find function classes from Jar", ioex);
      throw ioex;
    }
    String artifactId = JarFileUtils.toArtifactId(jarFile.getName());
    List<Function<?>> previouslyRegisteredFunctions =
        deploymentToFunctionsMap.remove(artifactId);
    if (!registeredFunctions.isEmpty()) {
      deploymentToFunctionsMap.put(artifactId, registeredFunctions);
    }
    unregisterUndeployedFunctions(previouslyRegisteredFunctions, registeredFunctions);
  }

  private void unregisterUndeployedFunctions(List<Function<?>> previouslyRegisteredFunctions,
      List<Function<?>> registeredFunctions) {

    if (previouslyRegisteredFunctions == null) {
      return;
    }

    List<String> currentlyRegisteredFunctionIDs =
        registeredFunctions.stream().map(Function::getId).collect(Collectors.toList());
    previouslyRegisteredFunctions.stream().map(Function::getId)
        .filter(functionId -> !currentlyRegisteredFunctionIDs.contains(functionId))
        .forEach(FunctionService::unregisterFunction);
  }

  private Collection<Function<?>> loadFunctionFromClassName(String className)
      throws ClassNotFoundException {
    Class<?> clazz = ClassPathLoader.getLatest().forName(className);
    return getRegisterableFunctionsFromClass(clazz);
  }

  private List<Function<?>> registerFunction(String jarFilePath,
      Collection<Function<?>> functions) {
    List<Function<?>> registeredFunctions = new ArrayList<>();
    for (Function<?> function : functions) {
      FunctionService.registerFunction(function);
      logger.debug("Registering function class: {}, from JAR file: {}",
          function.getClass().getName(), jarFilePath);
      registeredFunctions.add(function);
    }
    return registeredFunctions;
  }

  /**
   * Unregisters functions from a previously deployed jar that are not present in its replacement.
   * If newJar is null, all functions registered from oldJar will be removed.
   *
   * @param jarFileName - The name of the deployment that linked to the functions that are to be
   *        unregistered.
   */
  public void unregisterFunctionsForDeployment(String jarFileName) {
    List<Function<?>> functions =
        deploymentToFunctionsMap.remove(JarFileUtils.getArtifactId(jarFileName));
    if (functions != null) {
      functions.stream().map(Function::getId).forEach(FunctionService::unregisterFunction);
    }
  }

  private static Collection<String> findFunctionsInThisJar(File jarFile) throws IOException {
    return new FunctionScanner().findFunctionsInJar(jarFile);
  }

  /**
   * Check to see if the class implements the Function interface. If so, it will be registered with
   * FunctionService. Also, if the functions's class was originally declared in a cache.xml file
   * then any properties specified at that time will be reused when re-registering the function.
   *
   * @param clazz Class to check for implementation of the Function class
   * @return A collection of Objects that implement the Function interface.
   */
  @SuppressWarnings({"deprecation", "unchecked"})
  private Collection<Function<?>> getRegisterableFunctionsFromClass(Class<?> clazz) {
    final List<Function<?>> registerableFunctions = new ArrayList<>();

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
                Function<?> function = newFunction((Class<Function<?>>) clazz, true);
                if (function != null) {
                  ((Declarable) function).initialize(cache, properties);
                  ((Declarable) function).init(properties); // for backwards compatibility
                  if (function.getId() != null) {
                    registerableFunctions.add(function);
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
          Function<?> function = newFunction((Class<Function<?>>) clazz, false);
          if (function != null && function.getId() != null) {
            registerableFunctions.add(function);
          }
        }
      }
    } catch (Exception ex) {
      logger.error("Attempting to register function from class: {}", clazz, ex);
    }

    return registerableFunctions;
  }

  private Function<?> newFunction(final Class<Function<?>> clazz,
      final boolean errorOnNoSuchMethod) {
    try {
      final Constructor<Function<?>> constructor = clazz.getConstructor();
      return constructor.newInstance();
    } catch (NoSuchMethodException nsmex) {
      if (errorOnNoSuchMethod) {
        logger.error("Zero-arg constructor is required, but not found for class: {}",
            clazz.getName(), nsmex);
      } else {
        logger.debug(
            "Not registering function because it doesn't have a zero-arg constructor: {}",
            clazz.getName());
      }
    } catch (Exception ex) {
      logger.error("Error when attempting constructor for function for class: {}", clazz.getName(),
          ex);
    }

    return null;
  }
}
