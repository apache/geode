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

import java.io.File;
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
  private String packageToScan;

  public ClassScanner(String packageToScan) {
    this.packageToScan = packageToScan;
    scanResult = new ClassGraph().whitelistPackages(packageToScan)
        .enableClassInfo()
        .enableAnnotationInfo().scan();
  }

  public List<String> whatExtends(String classOrFile) {
    Set<String> classesToConsider = new HashSet<>();
    classesToConsider.add(pathToClass(classOrFile));

    if (!classOrFile.contains(".")) {
      classesToConsider.addAll(fullyQualifyClass(classOrFile));
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

  /**
   * Convert java file path into class name.
   */
  private String pathToClass(String javaFile) {
    String sanitized = javaFile.replace(File.separator, ".");

    int packageStart = sanitized.indexOf(packageToScan);
    if (packageStart >= 0) {
      sanitized = sanitized.substring(packageStart);
    }

    if (sanitized.endsWith(".java")) {
      int javaIdx = sanitized.indexOf(".java");
      sanitized = sanitized.substring(0, javaIdx);
    }

    return sanitized;
  }

  public static void main(String[] args) {
    ClassScanner scanner = new ClassScanner("org.apache.geode");

    Set<String> results = new HashSet<>();
    for (String arg : args) {
      List<String> extenders = scanner.whatExtends(arg);
      if (!extenders.isEmpty()) {
        results.addAll(extenders);
      } else {
        results.add(arg);
      }
    }

    System.out.println(String.join(" ", results));
  }

}
