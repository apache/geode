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
package org.apache.geode.management.internal.cli.util;

import static java.util.stream.Collectors.toSet;

import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import io.github.lukehutch.fastclasspathscanner.matchprocessor.ImplementingClassMatchProcessor;

/**
 * Utility class to scan class-path & load classes.
 *
 * @since GemFire 7.0
 */
public class ClasspathScanLoadHelper {

  public static <T> Set<Class<? extends T>> scanClasspathForClassesImplementing(
      Class<T> implementedInterface, String... packageSpec) {
    return scanPackageForClassesImplementing(implementedInterface, packageSpec);
  }

  public static <T> Set<Class<? extends T>> scanPackageForClassesImplementing(
      Class<T> implementedInterface, String... packageSpec) {
    Set<Class<? extends T>> classesImplementing = new HashSet<>();
    ImplementingClassMatchProcessor<T> matchProcessor = classesImplementing::add;

    new FastClasspathScanner(packageSpec)
        .matchClassesImplementing(implementedInterface, matchProcessor).scan();

    return classesImplementing.stream().filter(ClasspathScanLoadHelper::isInstantiable)
        .collect(toSet());
  }

  private static <T> boolean isInstantiable(Class<T> classToInstantiate) {
    int modifiers = classToInstantiate.getModifiers();

    return !Modifier.isAbstract(modifiers) && !Modifier.isInterface(modifiers)
        && Modifier.isPublic(modifiers);
  }
}
