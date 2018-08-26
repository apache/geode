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

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import io.github.classgraph.utils.WhiteBlackList;

/**
 * Utility class to scan class-path & load classes.
 *
 * @since GemFire 7.0
 */
public class ClasspathScanLoadHelper {

  private final ScanResult scanResult;

  public ClasspathScanLoadHelper(Collection<String> packagesToScan) {
    scanResult = new ClassGraph().whitelistPackages(packagesToScan.toArray(new String[] {}))
        .enableClassInfo()
        .enableAnnotationInfo().scan();
  }

  public Set<Class<?>> scanPackagesForClassesImplementing(Class<?> implementedInterface,
      String... onlyFromPackages) {
    ClassInfoList classInfoList = scanResult.getClassesImplementing(implementedInterface.getName())
        .filter(ci -> !ci.isAbstract() && !ci.isInterface() && ci.isPublic());

    classInfoList = classInfoList
        .filter(ci -> Arrays.stream(onlyFromPackages)
            .anyMatch(p -> classMatchesPackage(ci.getName(), p)));

    return classInfoList.loadClasses().stream().collect(toSet());
  }

  public Set<Class<?>> scanClasspathForAnnotation(Class<?> annotation, String... onlyFromPackages) {
    ClassInfoList classInfoList = scanResult.getClassesWithAnnotation(annotation.getName());

    classInfoList = classInfoList
        .filter(ci -> Arrays.stream(onlyFromPackages)
            .anyMatch(p -> classMatchesPackage(ci.getName(), p)));

    return classInfoList.loadClasses().stream().collect(toSet());
  }

  private boolean classMatchesPackage(String className, String packageSpec) {
    if (!packageSpec.contains("*")) {
      return className.startsWith(packageSpec);
    }

    Pattern globPattern = WhiteBlackList.globToPattern(packageSpec);
    return globPattern.matcher(className).matches();
  }

}
