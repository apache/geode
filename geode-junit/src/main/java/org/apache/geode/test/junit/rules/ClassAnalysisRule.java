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
package org.apache.geode.test.junit.rules;

import static java.util.Arrays.stream;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.apache.geode.codeAnalysis.CompiledClassUtils;
import org.apache.geode.codeAnalysis.decode.CompiledClass;

/**
 * ClassAnalysisRule will load all classes from the main src set of your module and
 * make them available to your test as CompiledClass instances via the getClasses()
 * method. The constructor for ClassAnalysisRule takes the name of your gradle
 * subproject, such as geode-core or geode-wan, and the rule uses this to locate
 * the build directory in order to load the classes.
 * <p>
 * This Rule is used by our AnalyzeSerializables unit tests.
 * <p>
 * NOTE: this rule is not thread-safe
 */
public class ClassAnalysisRule implements TestRule {

  private static final AtomicReference<Map<String, CompiledClass>> cachedClasses =
      new AtomicReference<>(new HashMap<>());

  private final String moduleName;
  private final String sourceSet;
  private final Map<String, CompiledClass> classes = new HashMap<>();

  /**
   * @param moduleName The name of the gradle module in which your test resides
   */
  public ClassAnalysisRule(String moduleName) {
    this(moduleName, "main");
  }

  public ClassAnalysisRule(String moduleName, String sourceSet) {
    this.moduleName = moduleName;
    this.sourceSet = sourceSet;
  }

  public Map<String, CompiledClass> getClasses() {
    return new HashMap<>(classes);
  }

  public static void clearCache() {
    cachedClasses.get().clear();
  }

  @Override
  public Statement apply(final Statement statement, final Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        before();
        statement.evaluate();
      }
    };
  }

  private void before() {
    if (cachedClasses.get().isEmpty()) {
      loadClasses();
      cachedClasses.get().putAll(classes);
    } else {
      classes.putAll(cachedClasses.get());
    }
  }

  private void loadClasses() {
    String classpath = System.getProperty("java.class.path");
    // System.out.println("java classpath is " + classpath);

    List<File> entries =
        stream(classpath.split(File.pathSeparator))
            .map(File::new)
            .collect(Collectors.toList());

    // check for <module>/build/classes/java/**
    String gradleBuildDirName =
        Paths.get(getModuleName(), "build", "classes", "java", sourceSet).toString();

    // check for <module>/build/classes/test/**
    String alternateBuildDirName =
        Paths.get(getModuleName(), "build", "classes", sourceSet).toString();

    // check for IntelliJ build location
    String intellijBuildDirName = getModuleName() + "." + sourceSet;

    String buildDir = null;
    for (File entry : entries) {
      if (entry.toString().endsWith(gradleBuildDirName)
          || entry.toString().endsWith(alternateBuildDirName)
          || entry.toString().endsWith(intellijBuildDirName)) {
        buildDir = entry.toString();
        break;
      }
    }
    assertThat(buildDir).isNotNull();
    System.out.println("ClassAnalysisRule is loading class files from " + buildDir);

    long start = System.currentTimeMillis();
    loadClassesFromBuild(new File(buildDir));
    long finish = System.currentTimeMillis();

    System.out.println("done loading " + classes.size() + " classes.  elapsed time = "
        + (finish - start) / 1000 + " seconds");
  }

  private String getModuleName() {
    return moduleName;
  }

  private void loadClassesFromBuild(File buildDir) {
    Map<String, CompiledClass> newClasses = CompiledClassUtils.parseClassFilesInDir(buildDir);
    classes.putAll(newClasses);
  }
}
