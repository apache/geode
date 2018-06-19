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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.Externalizable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import sun.reflect.ReflectionFactory;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.codeAnalysis.decode.CompiledClass;
import org.apache.geode.codeAnalysis.decode.CompiledField;
import org.apache.geode.codeAnalysis.decode.CompiledMethod;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributedSystemService;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({IntegrationTest.class, SerializationTest.class})
public class AnalyzeSerializablesJUnitTest {

  private static final String NEW_LINE = System.getProperty("line.separator");

  private static final String FAIL_MESSAGE = NEW_LINE + NEW_LINE
      + "If the class is not persisted or sent over the wire add it to the file " + NEW_LINE + "%s"
      + NEW_LINE + "Otherwise if this doesn't break backward compatibility, copy the file "
      + NEW_LINE + "%s to " + NEW_LINE + "%s.";
  private static final String EXCLUDED_CLASSES_TXT = "excludedClasses.txt";
  private static final String ACTUAL_DATA_SERIALIZABLES_DAT = "actualDataSerializables.dat";
  private static final String ACTUAL_SERIALIZABLES_DAT = "actualSerializables.dat";
  private static final String OPEN_BUGS_TXT = "openBugs.txt";

  /**
   * all loaded classes
   */
  private Map<String, CompiledClass> classes;

  private File expectedDataSerializablesFile;
  private String expectedSerializablesFileName =
      "sanctioned-" + getModuleName() + "-serializables.txt";
  private File expectedSerializablesFile;

  private List<ClassAndMethodDetails> expectedDataSerializables;
  private List<ClassAndVariableDetails> expectedSerializables;

  @Rule
  public TestName testName = new TestName();

  private void loadExpectedDataSerializables() throws Exception {
    this.expectedDataSerializablesFile = getResourceAsFile("sanctionedDataSerializables.txt");
    assertThat(this.expectedDataSerializablesFile).exists().canRead();

    this.expectedDataSerializables = loadClassesAndMethods(this.expectedDataSerializablesFile);
    Collections.sort(this.expectedDataSerializables);

  }

  public void loadExpectedSerializables() throws Exception {
    this.expectedSerializablesFile =
        getResourceAsFile(InternalDataSerializer.class, expectedSerializablesFileName);
    assertThat(this.expectedSerializablesFile).exists().canRead();

    this.expectedSerializables = loadClassesAndVariables(this.expectedSerializablesFile);
    Collections.sort(this.expectedSerializables);
  }

  public void findClasses() throws Exception {
    this.classes = new HashMap<>();

    loadClasses();
  }

  @Before
  public void setUp() throws Exception {
    assumeThat(
        "AnalyzeSerializables requires Java 8 but tests are running with v" + getJavaVersion(),
        isJavaVersionAtLeast("1.8"), is(true));
    TypeRegistry.init();
  }

  private List<DistributedSystemService> initializeServices() {
    ServiceLoader<DistributedSystemService> loader =
        ServiceLoader.load(DistributedSystemService.class);
    List<DistributedSystemService> services = new ArrayList<>();
    for (DistributedSystemService service : loader) {
      services.add(service);
    }
    return services;
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
    findClasses();
    loadExpectedDataSerializables();

    File actualDataSerializablesFile = createEmptyFile(ACTUAL_DATA_SERIALIZABLES_DAT);
    System.out.println(this.testName.getMethodName() + " actualDataSerializablesFile="
        + actualDataSerializablesFile.getAbsolutePath());

    List<ClassAndMethods> actualDataSerializables = findToDatasAndFromDatas();
    storeClassesAndMethods(actualDataSerializables, actualDataSerializablesFile);

    String diff =
        diffSortedClassesAndMethods(this.expectedDataSerializables, actualDataSerializables);
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
  public void testSerializables() throws Exception {
    System.out.println(this.testName.getMethodName() + " starting");
    findClasses();
    loadExpectedSerializables();

    File actualSerializablesFile = createEmptyFile(ACTUAL_SERIALIZABLES_DAT);
    System.out.println(this.testName.getMethodName() + " actualSerializablesFile="
        + actualSerializablesFile.getAbsolutePath());

    List<ClassAndVariables> actualSerializables = findSerializables();
    storeClassesAndVariables(actualSerializables, actualSerializablesFile);

    String diff = diffSortedClassesAndVariables(this.expectedSerializables, actualSerializables);
    if (!diff.isEmpty()) {
      System.out.println(
          "++++++++++++++++++++++++++++++testSerializables found discrepancies++++++++++++++++++++++++++++++++++++");
      System.out.println(diff);
      fail(diff + FAIL_MESSAGE, getSrcPathFor(getResourceAsFile(EXCLUDED_CLASSES_TXT)),
          actualSerializablesFile.getAbsolutePath(),
          getSrcPathFor(this.expectedSerializablesFile, "main"));
    }
  }

  @Test
  public void testExcludedClassesExistAndDoNotDeserialize() throws Exception {
    List<String> excludedClasses = loadExcludedClasses(getResourceAsFile(EXCLUDED_CLASSES_TXT));
    Properties properties = new Properties();
    properties.put(ConfigurationProperties.VALIDATE_SERIALIZABLE_OBJECTS, "true");
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER, "!*");
    DistributionConfig distributionConfig = new DistributionConfigImpl(properties);
    InternalDataSerializer.initialize(distributionConfig, initializeServices());

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


  private void serializeAndDeserializeObject(Object object) throws Exception {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(Version.CURRENT);
    try {
      DataSerializer.writeObject(object, outputStream);
    } catch (IOException e) {
      // some classes, such as BackupLock, are Serializable because the extend something
      // like ReentrantLock but we never serialize them & it doesn't work to try to do so
      System.out.println("Not Serializable: " + object.getClass().getName());
    }
    try {
      DataSerializer
          .readObject(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));
      fail("I was able to deserialize " + object.getClass().getName());
    } catch (InvalidClassException e) {
      // expected
    }
  }

  @Test
  public void testSanctionedClassesExistAndDoDeserialize() throws Exception {
    loadExpectedSerializables();
    Set<String> openBugs = new HashSet<>(loadOpenBugs(getResourceAsFile(OPEN_BUGS_TXT)));

    DistributionConfig distributionConfig = new DistributionConfigImpl(new Properties());
    distributionConfig.setValidateSerializableObjects(true);
    distributionConfig.setSerializableObjectFilter("!*");
    InternalDataSerializer.initialize(distributionConfig, initializeServices());

    for (ClassAndVariableDetails details : expectedSerializables) {
      if (openBugs.contains(details.className)) {
        System.out.println("Skipping " + details.className + " because it is in openBugs.txt");
        continue;
      }
      String className = details.className.replaceAll("/", ".");
      System.out.println("testing class " + details.className);

      Class sanctionedClass = null;
      try {
        sanctionedClass = Class.forName(className);
      } catch (ClassNotFoundException cnf) {
        fail(className + " cannot be found.  It may need to be removed from "
            + expectedSerializablesFileName);
      }

      assertTrue(
          sanctionedClass.getName() + " is not Serializable and should be removed from "
              + expectedSerializablesFileName,
          Serializable.class.isAssignableFrom(sanctionedClass));

      if (Modifier.isAbstract(sanctionedClass.getModifiers())) {
        // we detect whether these are modified in another test, but cannot instantiate them.
        continue;
      }

      if (sanctionedClass.getEnclosingClass() != null
          && sanctionedClass.getEnclosingClass().isEnum()) {
        // inner enum class - enum constants are handled when we process their enclosing class
        continue;
      }

      if (sanctionedClass.isEnum()) {
        // geode enums are special cased by DataSerializer and are never java-serialized
        for (Object instance : sanctionedClass.getEnumConstants()) {
          serializeAndDeserializeSanctionedObject(instance);
        }
        continue;
      }

      Object sanctionedInstance;
      if (!Serializable.class.isAssignableFrom(sanctionedClass)) {
        throw new AssertionError(
            className + " is not serializable.  Remove it from " + expectedSerializablesFileName);
      }
      try {
        boolean isThrowable = Throwable.class.isAssignableFrom(sanctionedClass);

        Constructor constructor = isThrowable ? sanctionedClass.getDeclaredConstructor(String.class)
            : sanctionedClass.getDeclaredConstructor((Class<?>[]) null);
        constructor.setAccessible(true);
        sanctionedInstance =
            isThrowable ? constructor.newInstance("test throwable") : constructor.newInstance();
        serializeAndDeserializeSanctionedObject(sanctionedInstance);
        continue;
      } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
        // fall through
      }
      try {
        Class<?> superClass = sanctionedClass;
        Constructor constructor = null;
        if (Externalizable.class.isAssignableFrom(sanctionedClass)) {
          Constructor<?> cons = sanctionedClass.getDeclaredConstructor((Class<?>[]) null);
          cons.setAccessible(true);
        } else {
          while (Serializable.class.isAssignableFrom(superClass)) {
            if ((superClass = superClass.getSuperclass()) == null) {
              throw new AssertionError(
                  className + " cannot be instantiated for serialization.  Remove it from "
                      + expectedSerializablesFileName);
            }
          }
          constructor = superClass.getDeclaredConstructor((Class<?>[]) null);
          constructor.setAccessible(true);
          constructor = ReflectionFactory.getReflectionFactory()
              .newConstructorForSerialization(sanctionedClass, constructor);
        }
        sanctionedInstance = constructor.newInstance();
      } catch (Exception e2) {
        throw new AssertionError("Unable to instantiate " + className + " - please move it from "
            + expectedSerializablesFileName + " to excludedClasses.txt", e2);
      }
      serializeAndDeserializeSanctionedObject(sanctionedInstance);
    }
  }

  @Test
  public void testOpenBugsAreInSanctionedSerializables() throws Exception {
    loadExpectedSerializables();
    List<String> openBugs = loadOpenBugs(getResourceAsFile(OPEN_BUGS_TXT));
    Set<String> expectedSerializableClasses = new HashSet<>();

    for (ClassAndVariableDetails details : expectedSerializables) {
      expectedSerializableClasses.add(details.className);
    }

    for (String openBugClass : openBugs) {
      assertTrue(
          "open bug class: " + openBugClass + " is not present in " + expectedSerializablesFileName,
          expectedSerializableClasses.contains(openBugClass));
    }
  }

  @Test
  public void testExcludedClassesAreNotInSanctionedSerializables() throws Exception {
    loadExpectedSerializables();
    Set<String> expectedSerializableClasses = new HashSet<>();

    for (ClassAndVariableDetails details : expectedSerializables) {
      expectedSerializableClasses.add(details.className);
    }

    List<String> excludedClasses = loadExcludedClasses(getResourceAsFile(EXCLUDED_CLASSES_TXT));

    for (String excludedClass : excludedClasses) {
      assertFalse(
          "Excluded class: " + excludedClass + " was found in " + expectedSerializablesFileName,
          expectedSerializableClasses.contains(excludedClass));
    }
  }

  private void serializeAndDeserializeSanctionedObject(Object object) throws Exception {
    HeapDataOutputStream outputStream = new HeapDataOutputStream(Version.CURRENT);
    try {
      DataSerializer.writeObject(object, outputStream);
    } catch (IOException e) {
      // some classes, such as BackupLock, are Serializable because the extend something
      // like ReentrantLock but we never serialize them & it doesn't work to try to do so
      throw new AssertionError("Not Serializable: " + object.getClass().getName(), e);
    }
    try {
      Object instance = DataSerializer
          .readObject(new DataInputStream(new ByteArrayInputStream(outputStream.toByteArray())));
    } catch (CancelException e) {
      // PDX classes fish for a PDXRegistry and find that there is no cache
    } catch (InvalidClassException e) {
      fail("I was unable to deserialize " + object.getClass().getName(), e);
    }
  }

  private String getSrcPathFor(File file) {
    return getSrcPathFor(file, "test");
  }

  private String getSrcPathFor(File file, String testOrMain) {
    return file.getAbsolutePath().replace(
        "build" + File.separator + "resources" + File.separator + "test",
        "src" + File.separator + testOrMain + File.separator + "resources");
  }

  private void loadClasses() throws IOException {
    System.out.println("loadClasses starting");

    List<String> excludedClasses = loadExcludedClasses(getResourceAsFile(EXCLUDED_CLASSES_TXT));
    List<String> openBugs = loadOpenBugs(getResourceAsFile(OPEN_BUGS_TXT));

    excludedClasses.addAll(openBugs);

    String classpath = System.getProperty("java.class.path");
    System.out.println("java classpath is " + classpath);

    String[] entries = classpath.split(File.pathSeparator);
    String gradleBuildDirName =
        Paths.get(getModuleName(), "build", "classes", "java", "main").toString();
    String ideaBuildDirName = Paths.get(getModuleName(), "out", "production", "classes").toString();
    String buildDir = null;

    for (String entry : entries) {
      System.out.println("examining '" + entry + "'");
      if (entry.endsWith(gradleBuildDirName) || entry.endsWith(ideaBuildDirName)) {
        buildDir = entry;
        break;
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

  private List<ClassAndVariables> findSerializables() throws IOException {
    List<ClassAndVariables> result = new ArrayList<>(2000);
    List<String> excludedClasses = loadExcludedClasses(getResourceAsFile(EXCLUDED_CLASSES_TXT));
    System.out.println("excluded classes are " + excludedClasses);
    Set<String> setOfExclusions = new HashSet<>(excludedClasses);
    for (Map.Entry<String, CompiledClass> entry : this.classes.entrySet()) {
      CompiledClass compiledClass = entry.getValue();
      if (setOfExclusions.contains(compiledClass.fullyQualifiedName())) {
        System.out.println("excluding class " + compiledClass.fullyQualifiedName());
        continue;
      }
      // System.out.println("processing class " + compiledClass.fullyQualifiedName());

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
    return getResourceAsFile(getClass(), resourceName);
  }

  private File getResourceAsFile(Class associatedClass, String resourceName) {
    return new File(associatedClass.getResource(resourceName).getFile());
  }
}
