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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.codeAnalysis.decode.CompiledClass;
import org.apache.geode.codeAnalysis.decode.CompiledMethod;
import org.apache.geode.internal.serialization.BufferDataOutputStream;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.test.junit.categories.SerializationTest;
import org.apache.geode.test.junit.rules.ClassAnalysisRule;

/**
 * This abstract test class is the basis for all of our AnalyzeModuleNameSerializables tests.
 * Subclasses must provide serialization/deserialization methods.
 * <p>
 * Most tests should subclass {@link AnalyzeSerializablesJUnitTestBase} instead of this
 * class because it ties into geode-core serialization and saves a lot of work.
 */
@Category({SerializationTest.class})
public abstract class AnalyzeDataSerializablesJUnitTestBase {

  private static final String NEW_LINE = System.getProperty("line.separator");

  protected static final String FAIL_MESSAGE = NEW_LINE + NEW_LINE
      + "If the class is not persisted or sent over the wire add it to the file " + NEW_LINE + "%s"
      + NEW_LINE + "Otherwise if this doesn't break backward compatibility, copy the file "
      + NEW_LINE + "%s to " + NEW_LINE + "%s.";
  protected static final String EXCLUDED_CLASSES_TXT = "excludedClasses.txt";
  private static final String ACTUAL_DATA_SERIALIZABLES_DAT = "actualDataSerializables.dat";
  protected static final String OPEN_BUGS_TXT = "openBugs.txt";

  /**
   * all loaded classes
   */
  protected Map<String, CompiledClass> classes;

  private File expectedDataSerializablesFile;

  private List<ClassAndMethodDetails> expectedDataSerializables;
  @Rule
  public TestName testName = new TestName();

  @Rule
  public ClassAnalysisRule classProvider = new ClassAnalysisRule(getModuleName());

  @AfterClass
  public static void afterClass() {
    ClassAnalysisRule.clearCache();
  }

  /**
   * implement this to return your module's name, such as "geode-core"
   */
  protected abstract String getModuleName();

  /**
   * Implement this method to return a production class in your module that corresponds to where
   * you have put your sanctioned-modulename-serializables.txt file in the production resources
   * tree.
   */
  protected abstract Class getModuleClass();

  /**
   * Implement this to deserialize an object that was serialized with serializeObject()
   */
  protected abstract void deserializeObject(BufferDataOutputStream outputStream)
      throws IOException, ClassNotFoundException;

  /**
   * Implement this to serialize the given object to the given output stream
   */
  protected abstract void serializeObject(Object object, BufferDataOutputStream outputStream)
      throws IOException;

  /**
   * Prepare your serialization service for use
   */
  protected abstract void initializeSerializationService();



  private void loadExpectedDataSerializables() throws Exception {
    this.expectedDataSerializablesFile = getResourceAsFile("sanctionedDataSerializables.txt");
    assertThat(this.expectedDataSerializablesFile).exists().canRead();

    this.expectedDataSerializables =
        CompiledClassUtils.loadClassesAndMethods(this.expectedDataSerializablesFile);
  }

  public void findClasses() throws Exception {
    classes = classProvider.getClasses();

    List<String> excludedClasses = loadExcludedClasses(getResourceAsFile(EXCLUDED_CLASSES_TXT));
    List<String> openBugs = loadOpenBugs(getResourceAsFile(OPEN_BUGS_TXT));

    excludedClasses.addAll(openBugs);
    removeExclusions(classes, excludedClasses);
  }


  @Test
  public void testDataSerializables() throws Exception {
    // assumeTrue("Ignoring this test when java version is 9 and above",
    // !SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9));
    System.out.println(this.testName.getMethodName() + " starting");
    findClasses();
    loadExpectedDataSerializables();

    File actualDataSerializablesFile = createEmptyFile(ACTUAL_DATA_SERIALIZABLES_DAT);
    System.out.println(this.testName.getMethodName() + " actualDataSerializablesFile="
        + actualDataSerializablesFile.getAbsolutePath());

    List<ClassAndMethods> actualDataSerializables = findToDatasAndFromDatas();
    CompiledClassUtils.storeClassesAndMethods(actualDataSerializables, actualDataSerializablesFile);

    String diff =
        CompiledClassUtils
            .diffSortedClassesAndMethods(this.expectedDataSerializables, actualDataSerializables);
    if (!diff.isEmpty()) {
      System.out.println(
          "++++++++++++++++++++++++++++++testDataSerializables found discrepancies++++++++++++++++++++++++++++++++++++");
      System.out.println(diff);
      fail(diff + FAIL_MESSAGE, getSrcPathFor(getResourceAsFile(EXCLUDED_CLASSES_TXT)),
          actualDataSerializablesFile.getAbsolutePath(),
          getSrcPathFor(this.expectedDataSerializablesFile));
    }
  }

  @Test
  public void testExcludedClassesExistAndDoNotDeserialize() throws Exception {
    List<String> excludedClasses = loadExcludedClasses(getResourceAsFile(EXCLUDED_CLASSES_TXT));

    initializeSerializationService();

    for (String filePath : excludedClasses) {
      String className = filePath.replaceAll("/", ".");
      System.out.println("testing class " + className);

      Class excludedClass = Class.forName(className);
      assertTrue(
          excludedClass.getName()
              + " is not Serializable and should be removed from excludedClasses.txt",
          Serializable.class.isAssignableFrom(excludedClass));

      if (!excludedClass.isEnum()) {
        final Object excludedInstance;
        try {
          excludedInstance = excludedClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          // okay - it's in the excludedClasses.txt file after all
          // IllegalAccessException means that the constructor is private.
          continue;
        }
        serializeAndDeserializeObject(excludedInstance);
      }
    }
  }



  protected void serializeAndDeserializeObject(Object object) throws Exception {
    BufferDataOutputStream outputStream = new BufferDataOutputStream(Version.CURRENT);
    try {
      serializeObject(object, outputStream);
    } catch (IOException e) {
      // some classes, such as BackupLock, are Serializable because the extend something
      // like ReentrantLock but we never serialize them & it doesn't work to try to do so
      System.out.println("Not Serializable: " + object.getClass().getName());
    }
    try {
      deserializeObject(outputStream);
      fail("I was able to deserialize " + object.getClass().getName());
    } catch (InvalidClassException e) {
      // expected
    }
  }

  protected String getSrcPathFor(File file) {
    return getSrcPathFor(file, "test");
  }

  private String getSrcPathFor(File file, String testOrMain) {
    return file.getAbsolutePath().replace(
        "build" + File.separator + "resources" + File.separator + "test",
        "src" + File.separator + testOrMain + File.separator + "resources");
  }

  protected List<String> loadExcludedClasses(File exclusionsFile) throws IOException {
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

  protected List<String> loadOpenBugs(File exclusionsFile) throws IOException {
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

  protected File createEmptyFile(String fileName) throws IOException {
    File file = new File(fileName);
    if (file.exists()) {
      assertThat(file.delete()).isTrue();
    }
    assertThat(file.createNewFile()).isTrue();
    assertThat(file).exists().canWrite();
    return file;
  }

  /**
   * Use this method to get a resource stored in the test's resource directory
   */
  protected File getResourceAsFile(String resourceName) {
    return new File(getClass().getResource(resourceName).getFile());
  }

  /**
   * Use this method to get a resource that might be in a JAR file
   */
  protected InputStream getResourceAsStream(Class associatedClass, String resourceName)
      throws IOException {
    return associatedClass.getResource(resourceName).openStream();
  }
}
