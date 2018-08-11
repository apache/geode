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

import java.util.Set;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

/**
 * Utility class to scan class-path & load classes.
 *
 * @since GemFire 7.0
 */
public class ClasspathScanLoadHelper {
  public static Set<Class<?>> scanPackagesForClassesImplementing(Class<?> implementedInterface,
      String... packagesToScan) {
    ScanResult scanResult = new ClassGraph().whitelistPackages(packagesToScan).enableClassInfo()
        .enableAnnotationInfo().scan();

    ClassInfoList classInfoList = scanResult.getClassesImplementing(implementedInterface.getName())
        .filter(ci -> !ci.isAbstract() && !ci.isInterface() && ci.isPublic());

    return classInfoList.loadClasses().stream().collect(toSet());
  }

  public static Set<Class<?>> scanClasspathForAnnotation(Class<?> annotation,
      String... packagesToScan) {
    ScanResult scanResult = new ClassGraph().whitelistPackages(packagesToScan).enableClassInfo()
        .enableAnnotationInfo().scan();
    ClassInfoList classInfoList = scanResult.getClassesWithAnnotation(annotation.getName());

    return classInfoList.loadClasses().stream().collect(toSet());
  }

}
