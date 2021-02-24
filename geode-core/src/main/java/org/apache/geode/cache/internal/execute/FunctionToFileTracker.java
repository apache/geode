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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.logging.log4j.Logger;
import org.apache.shiro.util.CollectionUtils;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.deployment.FunctionScanner;

/**
 * This class is used to register {@link Function}s from deployed jar files. It provides convenience
 * over working with the {@link FunctionService} directly.
 */
public class FunctionToFileTracker {

  private static final Pattern PATTERN_SLASH = Pattern.compile("/");

  private static final Logger logger = LogService.getLogger();

  private final Map<File, List<Function<?>>> fileToFunctionsMap = new ConcurrentHashMap<>();

  /**
   * Scan the JAR file and attempt to register any function classes found.
   *
   * @param jarFile deployed jar file to scan for {@link Function}s to register.
   */
  public synchronized void registerFunctions(File jarFile) throws ClassNotFoundException {
    logger.debug("Registering functions with DeployedJar: {}", jarFile);

    List<Function<?>> registeredFunctions = new LinkedList<>();

    BufferedInputStream bufferedInputStream;
    try {
      bufferedInputStream = new BufferedInputStream(new FileInputStream(jarFile));
    } catch (Exception ex) {
      logger.error("Unable to scan jar file for functions");
      return;
    }

    JarInputStream jarInputStream = null;
    try {
      Collection<String> functionClasses = findFunctionsInThisJar(jarFile);
      jarInputStream = new JarInputStream(bufferedInputStream);
      JarEntry jarEntry = jarInputStream.getNextJarEntry();

      while (jarEntry != null) {
        if (jarEntry.getName().endsWith(".class")) {
          final String className = PATTERN_SLASH.matcher(jarEntry.getName()).replaceAll("\\.")
              .substring(0, jarEntry.getName().length() - 6);

          if (functionClasses.contains(className)) {
            logger.debug("Attempting to load class: {}, from JAR file: {}", jarEntry.getName(),
                jarFile.getAbsolutePath());
            try {
              Class<?> clazz = ClassPathLoader.getLatest().forName(className);
              Collection<Function<?>> registerableFunctions =
                  getRegisterableFunctionsFromClass(clazz);
              for (Function<?> function : registerableFunctions) {
                FunctionService.registerFunction(function);
                logger.debug("Registering function class: {}, from JAR file: {}", className,
                    jarFile.getAbsolutePath());
                registeredFunctions.add(function);
              }
            } catch (ClassNotFoundException | NoClassDefFoundError cnfex) {
              logger.error("Unable to load all classes from JAR file: {}",
                  jarFile.getAbsolutePath(), cnfex);
              throw cnfex;
            }
          } else {
            logger.debug("No functions found in class: {}, from JAR file: {}", jarEntry.getName(),
                jarFile.getAbsolutePath());
          }
        }
        jarEntry = jarInputStream.getNextJarEntry();
      }
    } catch (IOException ioex) {
      logger.error("Exception when trying to read class from ByteArrayInputStream", ioex);
    } finally {
      if (jarInputStream != null) {
        try {
          jarInputStream.close();
        } catch (IOException ioex) {
          logger.error("Exception attempting to close JAR input stream", ioex);
        }
      }
      try {
        bufferedInputStream.close();
      } catch (IOException ignore) {
      }
    }

    fileToFunctionsMap.put(jarFile, registeredFunctions);
  }

  /**
   * Unregisters functions from a previously deployed jar that are not present in its replacement.
   * If newJar is null, all functions registered from oldJar will be removed.
   *
   * @param oldJar a previously deployed jar file from which functions maybe have been registered.
   * @param newJar a jar file that replaces oldJar.
   */
  public void unregisterRemovedFunctions(File oldJar, File newJar) {
    List<Function<?>> functions = fileToFunctionsMap.remove(oldJar);
    if (functions != null) {
      Stream<String> oldFunctions = functions.stream().map(Function::getId);

      Stream<String> removedFunctions;
      if (newJar == null) {
        removedFunctions = oldFunctions;
      } else {
        Predicate<String> isRemoved =
            (String oldFunctionId) -> !hasFunctionWithId(newJar, oldFunctionId);

        removedFunctions = oldFunctions.filter(isRemoved);
      }

      removedFunctions.forEach(FunctionService::unregisterFunction);
    }
  }

  private boolean hasFunctionWithId(File jarFile, String id) {
    List<Function<?>> functions = fileToFunctionsMap.get(jarFile);
    if (CollectionUtils.isEmpty(functions)) {
      return false;
    }

    return functions.stream().map(Function::getId).anyMatch(functionId -> functionId.equals(id));
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
