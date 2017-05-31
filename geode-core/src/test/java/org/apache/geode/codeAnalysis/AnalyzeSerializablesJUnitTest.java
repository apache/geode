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
package org.apache.geode.codeAnalysis;

import static org.apache.geode.codeAnalysis.CompiledClassUtils.diffSortedClassesAndMethods;
import static org.apache.geode.codeAnalysis.CompiledClassUtils.diffSortedClassesAndVariables;
import static org.apache.geode.codeAnalysis.CompiledClassUtils.loadClassesAndMethods;
import static org.apache.geode.codeAnalysis.CompiledClassUtils.loadClassesAndVariables;
import static org.apache.geode.codeAnalysis.CompiledClassUtils.parseClassFilesInDir;
import static org.apache.geode.codeAnalysis.CompiledClassUtils.storeClassesAndMethods;
import static org.apache.geode.codeAnalysis.CompiledClassUtils.storeClassesAndVariables;
import static org.apache.geode.internal.lang.SystemUtils.getJavaVersion;
import static org.apache.geode.internal.lang.SystemUtils.isJavaVersionAtLeast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeThat;

import org.apache.geode.codeAnalysis.decode.CompiledClass;
import org.apache.geode.codeAnalysis.decode.CompiledField;
import org.apache.geode.codeAnalysis.decode.CompiledMethod;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Category(IntegrationTest.class)
public class AnalyzeSerializablesJUnitTest {

  private static final String NEW_LINE = System.getProperty("line.separator");

  private static final String FAIL_MESSAGE = NEW_LINE + NEW_LINE
      + "If the class is not persisted or sent over the wire add it to the file " + NEW_LINE + "%s"
      + NEW_LINE + "Otherwise if this doesn't break backward compatibility, copy the file "
      + NEW_LINE + "%s to " + NEW_LINE + "%s.";

  /** all loaded classes */
  private Map<String, CompiledClass> classes;

  private File expectedDataSerializablesFile;
  private File expectedSerializablesFile;

  private List<ClassAndMethodDetails> expectedDataSerializables;
  private List<ClassAndVariableDetails> expectedSerializables;

  private File actualDataSerializablesFile;
  private File actualSerializablesFile;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    assumeThat(
        "AnalyzeSerializables requires Java 8 but tests are running with v" + getJavaVersion(),
        isJavaVersionAtLeast("1.8"), is(true));

    this.classes = new HashMap<>();

    loadClasses();

    // setup expectedDataSerializables

    this.expectedDataSerializablesFile = getResourceAsFile("sanctionedDataSerializables.txt");
    assertThat(this.expectedDataSerializablesFile).exists().canRead();

    this.expectedDataSerializables = loadClassesAndMethods(this.expectedDataSerializablesFile);
    Collections.sort(this.expectedDataSerializables);

    // setup expectedSerializables

    this.expectedSerializablesFile = getResourceAsFile("sanctionedSerializables.txt");
    assertThat(this.expectedSerializablesFile).exists().canRead();

    this.expectedSerializables = loadClassesAndVariables(this.expectedSerializablesFile);
    Collections.sort(this.expectedSerializables);

    // setup empty actual files

    this.actualDataSerializablesFile = createEmptyFile("actualDataSerializables.dat");
    this.actualSerializablesFile = createEmptyFile("actualSerializables.dat");
  }

  /**
   * Override only this one method in sub-classes
   */
  protected String getModuleName() {
    return "geode-core";
  }

  @Test
  public void testDataSerializables() throws Exception {
    System.out.println(this.testName.getMethodName() + " starting");

    List<ClassAndMethods> actualDataSerializables = findToDatasAndFromDatas();
    storeClassesAndMethods(actualDataSerializables, this.actualDataSerializablesFile);

    String diff =
        diffSortedClassesAndMethods(this.expectedDataSerializables, actualDataSerializables);
    if (!diff.isEmpty()) {
      System.out.println(
          "++++++++++++++++++++++++++++++testDataSerializables found discrepancies++++++++++++++++++++++++++++++++++++");
      System.out.println(diff);
      fail(diff + FAIL_MESSAGE, getSrcPathFor(getResourceAsFile("excludedClasses.txt")),
          this.actualDataSerializablesFile.getAbsolutePath(),
          getSrcPathFor(this.expectedDataSerializablesFile));
    }
  }

  @Test
  public void testSerializables() throws Exception {
    System.out.println(this.testName.getMethodName() + " starting");

    List<ClassAndVariables> actualSerializables = findSerializables();
    storeClassesAndVariables(actualSerializables, this.actualSerializablesFile);

    String diff = diffSortedClassesAndVariables(this.expectedSerializables, actualSerializables);
    if (!diff.isEmpty()) {
      System.out.println(
          "++++++++++++++++++++++++++++++testSerializables found discrepancies++++++++++++++++++++++++++++++++++++");
      System.out.println(diff);
      fail(diff + FAIL_MESSAGE, getSrcPathFor(getResourceAsFile("excludedClasses.txt")),
          this.actualSerializablesFile.getAbsolutePath(),
          getSrcPathFor(this.expectedSerializablesFile));
    }
  }

  private String getSrcPathFor(File file) {
    return file.getAbsolutePath().replace(
        "build" + File.separator + "resources" + File.separator + "test",
        "src" + File.separator + "test" + File.separator + "resources");
  }

  private void loadClasses() throws IOException {
    System.out.println("loadClasses starting");

    List<String> excludedClasses = loadExcludedClasses(getResourceAsFile("excludedClasses.txt"));
    List<String> openBugs = loadOpenBugs(getResourceAsFile("openBugs.txt"));

    excludedClasses.addAll(openBugs);

    String classpath = System.getProperty("java.class.path");
    System.out.println("java classpath is " + classpath);

    String[] entries = classpath.split(File.pathSeparator);
    String buildDirName = getModuleName() + File.separatorChar + "build" + File.separatorChar
        + "classes" + File.separatorChar + "main";
    String buildDir = null;

    for (int i = 0; i < entries.length && buildDir == null; i++) {
      System.out.println("examining '" + entries[i] + "'");
      if (entries[i].endsWith(buildDirName)) {
        buildDir = entries[i];
      }
    }

    assertThat(buildDir).isNotNull();
    System.out.println("loading class files from " + buildDir);

    long start = System.currentTimeMillis();
    loadClassesFromBuild(new File(buildDir), excludedClasses);
    long finish = System.currentTimeMillis();

    System.out.println("done loading " + this.classes.size() + " classes.  elapsed time = "
        + (finish - start) / 1000 + " seconds");
  }

  private List<String> loadExcludedClasses(File exclusionsFile) throws IOException {
    List<String> excludedClasses = new LinkedList<>();
    FileReader fr = new FileReader(exclusionsFile);
    BufferedReader br = new BufferedReader(fr);
    try {
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (!line.isEmpty() && !line.startsWith("#")) {
          excludedClasses.add(line);
        }
      }
    } finally {
      fr.close();
    }
    return excludedClasses;
  }

  private List<String> loadOpenBugs(File exclusionsFile) throws IOException {
    List<String> excludedClasses = new LinkedList<>();
    FileReader fr = new FileReader(exclusionsFile);
    BufferedReader br = new BufferedReader(fr);
    try {
      String line;
      // each line should have bug#,full-class-name
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (!line.isEmpty() && !line.startsWith("#")) {
          String[] split = line.split(",");
          if (split.length != 2) {
            fail("unable to load classes due to malformed line in openBugs.txt: " + line);
          }
          excludedClasses.add(line.split(",")[1].trim());
        }
      }
    } finally {
      fr.close();
    }
    return excludedClasses;
  }

  private void removeExclusions(Map<String, CompiledClass> classes, List<String> exclusions) {
    for (String exclusion : exclusions) {
      exclusion = exclusion.replace('.', '/');
      classes.remove(exclusion);
    }
  }

  private void loadClassesFromBuild(File buildDir, List<String> excludedClasses) {
    Map<String, CompiledClass> newClasses = parseClassFilesInDir(buildDir);
    removeExclusions(newClasses, excludedClasses);
    this.classes.putAll(newClasses);
  }

  private List<ClassAndMethods> findToDatasAndFromDatas() {
    List<ClassAndMethods> result = new ArrayList<>();
    for (Map.Entry<String, CompiledClass> entry : this.classes.entrySet()) {
      CompiledClass compiledClass = entry.getValue();
      ClassAndMethods classAndMethods = null;

      for (int i = 0; i < compiledClass.methods.length; i++) {
        CompiledMethod method = compiledClass.methods[i];

        if (!method.isAbstract() && method.descriptor().equals("void")) {
          String name = method.name();
          if (name.startsWith("toData") || name.startsWith("fromData")) {
            if (classAndMethods == null) {
              classAndMethods = new ClassAndMethods(compiledClass);
            }
            classAndMethods.methods.put(method.name(), method);
          }
        }
      }
      if (classAndMethods != null) {
        result.add(classAndMethods);
      }
    }
    Collections.sort(result);
    return result;
  }

  private List<ClassAndVariables> findSerializables() {
    List<ClassAndVariables> result = new ArrayList<>(2000);
    for (Map.Entry<String, CompiledClass> entry : this.classes.entrySet()) {
      CompiledClass compiledClass = entry.getValue();
      System.out.println("processing class " + compiledClass.fullyQualifiedName());

      if (!compiledClass.isInterface() && compiledClass.isSerializableAndNotDataSerializable()) {
        ClassAndVariables classAndVariables = new ClassAndVariables(compiledClass);
        for (int i = 0; i < compiledClass.fields_count; i++) {
          CompiledField compiledField = compiledClass.fields[i];
          if (!compiledField.isStatic() && !compiledField.isTransient()) {
            classAndVariables.variables.put(compiledField.name(), compiledField);
          }
        }
        result.add(classAndVariables);
      }
    }
    Collections.sort(result);
    return result;
  }

  private File createEmptyFile(String fileName) throws IOException {
    File file = new File(fileName);
    if (file.exists()) {
      assertThat(file.delete()).isTrue();
    }
    assertThat(file.createNewFile()).isTrue();
    assertThat(file).exists().canWrite();
    return file;
  }

  private File getResourceAsFile(String resourceName) {
    return new File(getClass().getResource(resourceName).getFile());
  }
}
