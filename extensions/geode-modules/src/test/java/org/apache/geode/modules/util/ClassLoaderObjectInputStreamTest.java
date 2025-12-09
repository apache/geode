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
package org.apache.geode.modules.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.Enumeration;
import java.util.Vector;

import org.apache.bcel.Const;
import org.apache.bcel.classfile.JavaClass;
import org.apache.bcel.generic.ClassGen;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;


public class ClassLoaderObjectInputStreamTest {
  private String classToLoad;
  private ClassLoader newTCCL;
  private ClassLoader originalTCCL;
  private Object instanceOfTCCLClass;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    originalTCCL = Thread.currentThread().getContextClassLoader();
    newTCCL = new GeneratingClassLoader();
    classToLoad = "com.nowhere." + getClass().getSimpleName() + "_" + testName.getMethodName();
    instanceOfTCCLClass = createInstanceOfTCCLClass();
  }

  @After
  public void unsetTCCL() {
    Thread.currentThread().setContextClassLoader(originalTCCL);
  }

  @Test
  public void resolveClassFromTCCLThrowsIfTCCLDisabled() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(instanceOfTCCLClass);
    oos.close();

    ObjectInputStream ois = new ClassLoaderObjectInputStream(
        new ByteArrayInputStream(baos.toByteArray()), getClass().getClassLoader());

    assertThatThrownBy(ois::readObject).isExactlyInstanceOf(ClassNotFoundException.class);
  }

  @Test
  public void resolveClassFindsClassFromTCCLIfTCCLEnabled() throws Exception {
    Thread.currentThread().setContextClassLoader(newTCCL);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(instanceOfTCCLClass);
    oos.close();

    ObjectInputStream ois = new ClassLoaderObjectInputStream(
        new ByteArrayInputStream(baos.toByteArray()), getClass().getClassLoader());

    Object objectFromTCCL = ois.readObject();

    assertThat(objectFromTCCL).isNotNull();
    assertThat(objectFromTCCL.getClass()).isNotNull();
    assertThat(objectFromTCCL.getClass().getName()).isEqualTo(classToLoad);
  }

  private Object createInstanceOfTCCLClass()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    Class<?> clazz = Class.forName(classToLoad, false, newTCCL);
    return clazz.newInstance();
  }

  /**
   * Custom class loader which uses BCEL to always dynamically generate a class for any class name
   * it tries to load.
   */
  private static class GeneratingClassLoader extends ClassLoader {

    /**
     * Currently unused but potentially useful for some future test. This causes this loader to only
     * generate a class that the parent could not find.
     *
     * @param parent the parent class loader to check with first
     */
    @SuppressWarnings("unused")
    public GeneratingClassLoader(ClassLoader parent) {
      super(parent);
    }

    /**
     * Specifies no parent to ensure that this loader generates the named class.
     */
    GeneratingClassLoader() {
      super(null); // no parent
    }

    @Override
    protected Class<?> findClass(String name) {
      ClassGen cg = new ClassGen(name, Object.class.getName(), "<generated>",
          Const.ACC_PUBLIC | Const.ACC_SUPER, new String[] {Serializable.class.getName()});
      cg.addEmptyConstructor(Const.ACC_PUBLIC);
      JavaClass jClazz = cg.getJavaClass();
      byte[] bytes = jClazz.getBytes();
      return defineClass(jClazz.getClassName(), bytes, 0, bytes.length);
    }

    @Override
    protected URL findResource(String name) {
      URL url;
      try {
        url = getTempFile().getAbsoluteFile().toURI().toURL();
        System.out.println("GeneratingClassLoader#findResource returning " + url);
      } catch (IOException e) {
        throw new Error(e);
      }
      return url;
    }

    @Override
    protected Enumeration<URL> findResources(String name) {
      URL url;
      try {
        url = getTempFile().getAbsoluteFile().toURI().toURL();
        System.out.println("GeneratingClassLoader#findResources returning " + url);
      } catch (IOException e) {
        throw new Error(e);
      }
      Vector<URL> urls = new Vector<>();
      urls.add(url);
      return urls.elements();
    }

    File getTempFile() {
      return null;
    }
  }

  @Test
  public void filterRejectsUnauthorizedClasses() throws Exception {
    // Arrange: Create filter that only allows java.lang and java.util classes
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter("java.lang.*;java.util.*;!*");
    TestSerializable testObject = new TestSerializable("test");
    byte[] serializedData = serialize(testObject);

    // Act & Assert: Deserialization should be rejected by filter
    assertThatThrownBy(() -> {
      try (ClassLoaderObjectInputStream ois = new ClassLoaderObjectInputStream(
          new ByteArrayInputStream(serializedData),
          Thread.currentThread().getContextClassLoader(),
          filter)) {
        ois.readObject();
      }
    }).isInstanceOf(InvalidClassException.class);
  }

  @Test
  public void filterAllowsAuthorizedClasses() throws Exception {
    // Arrange: Create filter that allows this test class package
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter(
        "java.lang.*;java.util.*;org.apache.geode.modules.util.**;!*");
    TestSerializable testObject = new TestSerializable("test data");
    byte[] serializedData = serialize(testObject);

    // Act: Deserialize with filter
    Object deserialized;
    try (ClassLoaderObjectInputStream ois = new ClassLoaderObjectInputStream(
        new ByteArrayInputStream(serializedData),
        Thread.currentThread().getContextClassLoader(),
        filter)) {
      deserialized = ois.readObject();
    }

    // Assert: Object should be successfully deserialized
    assertThat(deserialized).isInstanceOf(TestSerializable.class);
    assertThat(((TestSerializable) deserialized).getData()).isEqualTo("test data");
  }

  @Test
  public void nullFilterAllowsAllClasses() throws Exception {
    // Arrange: Null filter means no filtering (backward compatibility)
    TestSerializable testObject = new TestSerializable("unfiltered data");
    byte[] serializedData = serialize(testObject);

    // Act: Deserialize with null filter
    Object deserialized;
    try (ClassLoaderObjectInputStream ois = new ClassLoaderObjectInputStream(
        new ByteArrayInputStream(serializedData),
        Thread.currentThread().getContextClassLoader(),
        null)) {
      deserialized = ois.readObject();
    }

    // Assert: Object should be successfully deserialized
    assertThat(deserialized).isInstanceOf(TestSerializable.class);
    assertThat(((TestSerializable) deserialized).getData()).isEqualTo("unfiltered data");
  }

  @Test
  public void deprecatedConstructorStillWorks() throws Exception {
    // Arrange: Use deprecated constructor without filter
    TestSerializable testObject = new TestSerializable("legacy code");
    byte[] serializedData = serialize(testObject);

    // Act: Deserialize using deprecated constructor
    Object deserialized;
    try (ClassLoaderObjectInputStream ois = new ClassLoaderObjectInputStream(
        new ByteArrayInputStream(serializedData),
        Thread.currentThread().getContextClassLoader())) {
      deserialized = ois.readObject();
    }

    // Assert: Object should be successfully deserialized (backward compatibility)
    assertThat(deserialized).isInstanceOf(TestSerializable.class);
    assertThat(((TestSerializable) deserialized).getData()).isEqualTo("legacy code");
  }

  @Test
  public void filterEnforcesResourceLimits() throws Exception {
    // Arrange: Create filter with very low depth limit
    ObjectInputFilter filter = ObjectInputFilter.Config.createFilter("maxdepth=2;*");
    NestedSerializable nested = new NestedSerializable(
        new NestedSerializable(
            new NestedSerializable(null))); // Depth of 3
    byte[] serializedData = serialize(nested);

    // Act & Assert: Should reject due to depth limit
    assertThatThrownBy(() -> {
      try (ClassLoaderObjectInputStream ois = new ClassLoaderObjectInputStream(
          new ByteArrayInputStream(serializedData),
          Thread.currentThread().getContextClassLoader(),
          filter)) {
        ois.readObject();
      }
    }).isInstanceOf(InvalidClassException.class);
  }

  /**
   * Helper method to serialize an object to byte array
   */
  private byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(obj);
    }
    return baos.toByteArray();
  }

  /**
   * Test class for serialization testing
   */
  static class TestSerializable implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String data;

    TestSerializable(String data) {
      this.data = data;
    }

    String getData() {
      return data;
    }
  }

  /**
   * Nested test class for depth limit testing
   */
  static class NestedSerializable implements Serializable {
    private static final long serialVersionUID = 1L;
    private final NestedSerializable nested;

    NestedSerializable(NestedSerializable nested) {
      this.nested = nested;
    }
  }
}
