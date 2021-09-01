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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.apache.geode.codeAnalysis.CompiledClassUtils;
import org.apache.geode.codeAnalysis.decode.CompiledClass;
import org.apache.geode.test.junit.rules.serializable.SerializableStatement;


/**
 * ClassAnalysisRule will load all of the production classes from your module and
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
  private static final Map<String, CompiledClass> cachedClasses = new HashMap<>();

  private final String moduleName;
  private Map<String, CompiledClass> classes = new HashMap<>();

  /**
   * @param moduleName The name of the gradle module in which your test resides
   */
  public ClassAnalysisRule(String moduleName) {
    super();
    this.moduleName = moduleName;
  }

  public Map<String, CompiledClass> getClasses() {
    return new HashMap<>(classes);
  }

  public static void clearCache() {
    cachedClasses.clear();
  }


  @Override
  public Statement apply(final Statement statement, final Description description) {
    return new SerializableStatement() {
      @Override
      public void evaluate() throws Throwable {
        before();
        try {
          statement.evaluate();
        } finally {
          after();
        }
      }
    };
  }

  private void before() {
    if (cachedClasses.isEmpty()) {
      loadClasses();
      cachedClasses.putAll(classes);
    } else {
      classes.putAll(cachedClasses);
    }
  }

  private void after() {}

  private void loadClasses() {
    String classpath = System.getProperty("java.class.path");
    // System.out.println("java classpath is " + classpath);

    List<File> entries =
        Arrays.stream(classpath.split(File.pathSeparator)).map(x -> new File(x)).collect(
            Collectors.toList());
    String gradleBuildDirName =
        Paths.get(getModuleName(), "build", "classes", "java", "main").toString();
    // System.out.println("gradleBuildDirName is " + gradleBuildDirName);
    String ideaBuildDirName = Paths.get(getModuleName(), "out", "production", "classes").toString();
    // System.out.println("ideaBuildDirName is " + ideaBuildDirName);
    String ideaFQCNBuildDirName = Paths.get("out", "production",
        "geode." + getModuleName() + ".main").toString();
    // System.out.println("idea build path with full package names is " + ideaFQCNBuildDirName);
    String buildDir = null;

    for (File entry : entries) {
      if (entry.toString().endsWith(gradleBuildDirName)
          || entry.toString().endsWith(ideaBuildDirName)
          || entry.toString().endsWith(ideaFQCNBuildDirName)) {
        buildDir = entry.toString();
        break;
      }
    }

    assertThat(buildDir).isNotNull();
    System.out.println("ClassAnalysisRule is loading class files from " + buildDir);

    long start = System.currentTimeMillis();
    loadClassesFromBuild(new File(buildDir));
    long finish = System.currentTimeMillis();

    System.out.println("done loading " + this.classes.size() + " classes.  elapsed time = "
        + (finish - start) / 1000 + " seconds");
  }

  private String getModuleName() {
    return moduleName;
  }

  private void loadClassesFromBuild(File buildDir) {
    Map<String, CompiledClass> newClasses = CompiledClassUtils.parseClassFilesInDir(buildDir);
    this.classes.putAll(newClasses);
  }

}
