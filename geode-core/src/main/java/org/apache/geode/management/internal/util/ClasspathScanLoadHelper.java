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
package org.apache.geode.management.internal.util;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isInterface;
import static java.lang.reflect.Modifier.isPublic;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.reflections.Reflections;

/**
 * Utility class to scan class-path & load classes.
 *
 * @since GemFire 7.0
 */
public class ClasspathScanLoadHelper {
  private final Reflections reflections;

  public ClasspathScanLoadHelper(Collection<String> packagesToScan) {
    reflections = new Reflections(packagesToScan);
  }

  public Set<Class<?>> scanPackagesForClassesImplementing(Class<?> implementedInterface,
      String... onlyFromPackages) {
    return reflections.getSubTypesOf(implementedInterface)
        .stream()
        .filter(ci -> !isAbstract(ci.getModifiers()) && !isInterface(ci.getModifiers())
            && isPublic(ci.getModifiers()))
        .filter(ci -> Arrays.stream(onlyFromPackages)
            .anyMatch(p -> classMatchesPackage(ci.getName(), p)))
        .collect(Collectors.toSet());
  }

  public Set<Class<?>> scanClasspathForAnnotation(Class<? extends Annotation> annotation,
      String... onlyFromPackages) {
    return reflections.getTypesAnnotatedWith(annotation)
        .stream()
        .filter(ci -> Arrays.stream(onlyFromPackages)
            .anyMatch(p -> classMatchesPackage(ci.getName(), p)))
        .collect(Collectors.toSet());
  }

  /**
   * replaces shell-style glob characters with their regexp equivalents
   */
  private static String globToRegex(final String glob) {
    return "^" + glob.replace(".", "\\.").replace("*", ".*") + "$";
  }

  private static boolean classMatchesPackage(String className, String packageSpec) {
    if (!packageSpec.contains("*")) {
      return className.startsWith(packageSpec);
    }

    return className.matches(globToRegex(packageSpec));
  }
}
