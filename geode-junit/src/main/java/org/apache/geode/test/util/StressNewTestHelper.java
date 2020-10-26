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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;

/**
 * This class is intended as a helper to the CI StressNewTest job. Given a list of changed test
 * java files, it expands the list to include any subclasses and outputs a partial Gradle command
 * line to execute those tests depending on the 'category' of test (unit, distributed, etc.).
 */
public class StressNewTestHelper {

  private ScanResult scanResult;
  private String packageToScan;

  // Mapping of source set to list of tests
  private Map<String, List<String>> sourceToTestMapping = new HashMap<>();

  private static final Pattern categoryPattern = Pattern.compile(".*/src/(.*?)/java/.*");
  private static final Pattern intellijCategoryPattern =
      Pattern.compile(".*/out/test/.*\\.(.*?)/.*");
  private static final Pattern gradleCategoryPattern =
      Pattern.compile(".*/build/classes/java/(.*?)/.*");

  private static final Map<String, String> sourceToGradleMapping = new HashMap<>();
  static {
    sourceToGradleMapping.put("test", "repeatUnitTest");
    sourceToGradleMapping.put("integrationTest", "repeatIntegrationTest");
    sourceToGradleMapping.put("distributedTest", "repeatDistributedTest");
    sourceToGradleMapping.put("upgradeTest", "repeatUpgradeTest");
    // Cannot currently be run repeatedly because of docker issues
    // sourceToGradleMapping.put("acceptanceTest", "repeatAcceptanceTest");
  }

  private static class TestClassInfo {
    final String originalFilename;
    final String category;
    final String className;
    final String simpleClassName;

    TestClassInfo(String originalFilename, String category, String className,
        String simpleClassName) {
      this.originalFilename = originalFilename;
      this.category = category;
      this.className = className;
      this.simpleClassName = simpleClassName;
    }
  }

  public StressNewTestHelper(String packageToScan) {
    this.packageToScan = packageToScan;
    scanResult = new ClassGraph().whitelistPackages(packageToScan)
        .enableClassInfo()
        .enableAnnotationInfo().scan();
    System.out.println("DHE: test class map");
    scanResult.getAllClassesAsMap().entrySet().stream()
        .map(e -> String.format("    %s: %s", e.getKey(), e.getValue()))
        .sorted()
        .forEach(System.out::println);
  }

  public String buildGradleCommand() {
    StringBuilder command = new StringBuilder();

    int testCount = 0;
    for (Map.Entry<String, List<String>> entry : sourceToTestMapping.entrySet()) {
      String sourceSet = entry.getKey();
      if (sourceToGradleMapping.get(sourceSet) == null) {
        System.err.println("Skipping repeat test for " + sourceSet);
        continue;
      }

      command.append(sourceToGradleMapping.get(sourceSet));
      command.append(" --tests ");
      command.append(String.join(",", entry.getValue()));
      command.append(" ");
      testCount += entry.getValue().size();
    }

    // This exists so that scripts processing this output can extract the number of tests
    // included here. Yes, it's pretty hacky...
    command.append("-PtestCount=" + testCount);

    return command.toString();
  }

  public void add(String javaFile) {
    TestClassInfo testClassInfo = createTestClassInfo(javaFile);
    List<TestClassInfo> extenders = whatExtends(testClassInfo);

    String className = testClassInfo.className;
    ClassInfo classInfo = scanResult.getClassInfo(className);
    System.out
        .printf("DHE: StressNewTestHelper.add(%s): className %s classInfo %s%n", javaFile,
            className, classInfo);
    if (!classInfo.isAbstract()) {
      extenders.add(testClassInfo);
    }

    if (extenders.isEmpty()) {
      addTestToCategory(testClassInfo.category, testClassInfo.simpleClassName);
      return;
    }

    extenders.forEach(e -> addTestToCategory(e.category, e.simpleClassName));
  }

  private void addTestToCategory(String category, String testClass) {
    List<String> listOfTests =
        sourceToTestMapping.computeIfAbsent(category, k -> new ArrayList<>());
    listOfTests.add(testClass);
  }

  private List<TestClassInfo> whatExtends(TestClassInfo testClass) {
    List<TestClassInfo> results = new ArrayList<>();
    ClassInfoList subClasses = scanResult.getSubclasses(testClass.className);

    for (ClassInfo classInfo : subClasses) {
      String classFilename = classInfo.getClasspathElementURL().getFile();
      results.add(
          new TestClassInfo(classFilename, getCategory(classFilename), classInfo.getName(),
              classInfo.getSimpleName()));
    }

    return results;
  }

  private TestClassInfo createTestClassInfo(String javaFile) {
    String category = getCategory(javaFile);
    String sanitized = javaFile.replace(File.separator, ".");

    int packageStart = sanitized.indexOf(packageToScan);
    if (packageStart >= 0) {
      sanitized = sanitized.substring(packageStart);
    }

    if (sanitized.endsWith(".java")) {
      int javaIdx = sanitized.indexOf(".java");
      sanitized = sanitized.substring(0, javaIdx);
    }

    int classIndex = sanitized.lastIndexOf(".");

    return new TestClassInfo(javaFile, category, sanitized, sanitized.substring(classIndex + 1));
  }

  private String getCategory(String javaFile) {
    Matcher matcher = categoryPattern.matcher(javaFile);

    // Maybe we're running tests in Intellij
    if (!matcher.matches()) {
      matcher = intellijCategoryPattern.matcher(javaFile);
    }

    // Maybe we're running tests in Gradle
    if (!matcher.matches()) {
      matcher = gradleCategoryPattern.matcher(javaFile);
    }

    if (!matcher.matches()) {
      throw new IllegalArgumentException("Unable to determine category for " + javaFile);
    }

    return matcher.group(1);
  }

  public static void main(String[] args) {
    StressNewTestHelper helper = new StressNewTestHelper("org.apache.geode");

    for (String arg : args) {
      try {
        helper.add(arg);
      } catch (Exception e) {
        System.err.println("ERROR: Unable to process " + arg + " : " + e.getMessage());
      }
    }

    System.out.println(helper.buildGradleCommand());
  }

}
