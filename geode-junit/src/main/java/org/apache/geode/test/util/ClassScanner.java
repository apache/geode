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

package org.apache.geode.test.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ScanResult;

/**
 * This class is intended as a helper to the CI StressNewTest job.
 */
public class ClassScanner {

  private ScanResult scanResult;

  public ClassScanner(Set<String> packagesToScan) {
    scanResult = new ClassGraph().whitelistPackages(packagesToScan.toArray(new String[] {}))
        .enableClassInfo()
        .enableAnnotationInfo().scan();
  }

  public List<String> whatExtends(String className) {
    Set<String> classesToConsider = new HashSet<>();
    classesToConsider.add(className);

    if (!className.contains(".")) {
      classesToConsider.addAll(fullyQualifyClass(className));
    }

    return classesToConsider.stream()
        .flatMap(x -> scanResult.getSubclasses(x).stream().map(ClassInfo::getSimpleName))
        .collect(Collectors.toList());
  }

  public List<String> fullyQualifyClass(String simpleClass) {
    return scanResult.getAllClassesAsMap().keySet().stream()
        .filter(x -> x.endsWith(simpleClass))
        .collect(Collectors.toList());
  }

  public static void main(String[] args) {
    Set<String> packagesToScan = new HashSet<>();
    packagesToScan.add("org.apache.geode");

    ClassScanner scanner = new ClassScanner(packagesToScan);

    Set<String> result = Arrays.stream(args)
        .flatMap(x -> scanner.whatExtends(x).stream())
        .collect(Collectors.toSet());

    System.out.println(String.join(" ", result));
  }
}
