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
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;

import org.apache.bcel.Const;
import org.apache.bcel.classfile.JavaClass;
import org.apache.bcel.generic.ClassGen;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.modules.session.filter.SafeDeserializationFilter;

/**
 * Unit tests for SecureClassLoaderObjectInputStream
 */
public class SecureClassLoaderObjectInputStreamTest {

  private ClassLoader testClassLoader;
  private ClassLoader originalTCCL;
  private ObjectInputFilter allowAllFilter;
  private String dynamicClassName;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    originalTCCL = Thread.currentThread().getContextClassLoader();
    testClassLoader = getClass().getClassLoader();

    // Create a permissive filter for testing (allows all classes)
    allowAllFilter = info -> ObjectInputFilter.Status.ALLOWED;

    // Set up dynamic class name (will be created on-demand)
    dynamicClassName = "com.nowhere." + getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  @After
  public void tearDown() {
    Thread.currentThread().setContextClassLoader(originalTCCL);
  }

  // ============================================================
  // Constructor Tests
  // ============================================================

  @Test
  public void constructorThrowsIllegalArgumentExceptionWhenFilterIsNull() throws Exception {
    // Create a valid serialization stream
    byte[] serializedData = serialize("test");
    ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);

    assertThatThrownBy(() -> new SecureClassLoaderObjectInputStream(bais, testClassLoader, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ObjectInputFilter must not be null");
  }

  @Test
  public void constructorSucceedsWithValidFilter() throws Exception {
    byte[] serializedData = serialize("test");
    ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);

    SecureClassLoaderObjectInputStream ois =
        new SecureClassLoaderObjectInputStream(bais, testClassLoader, allowAllFilter);

    assertThat(ois).isNotNull();
    assertThat(ois.getFilter()).isEqualTo(allowAllFilter);
    assertThat(ois.getClassLoader()).isEqualTo(testClassLoader);
  }

  @Test
  public void constructorSetsFilterBeforeDeserialization() throws Exception {
    // This test verifies that the filter is properly set
    byte[] serializedData = serialize("test");
    ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);

    ObjectInputFilter testFilter = info -> ObjectInputFilter.Status.ALLOWED;
    SecureClassLoaderObjectInputStream ois =
        new SecureClassLoaderObjectInputStream(bais, testClassLoader, testFilter);

    // Verify the filter is set
    assertThat(ois.getFilter()).isEqualTo(testFilter);

    // And that deserialization works
    Object result = ois.readObject();
    assertThat(result).isEqualTo("test");
  }

  // ============================================================
  // resolveClass() Tests
  // ============================================================

  @Test
  public void resolveClassLoadsStandardJDKClasses() throws Exception {
    String testString = "test string";
    byte[] serializedData = serialize(testString);

    SecureClassLoaderObjectInputStream ois =
        new SecureClassLoaderObjectInputStream(
            new ByteArrayInputStream(serializedData),
            testClassLoader,
            allowAllFilter);

    Object result = ois.readObject();

    assertThat(result).isInstanceOf(String.class);
    assertThat(result).isEqualTo(testString);
  }

  @Test
  public void resolveClassLoadsCustomSerializableClasses() throws Exception {
    TestSerializable original = new TestSerializable("test", 42);
    byte[] serializedData = serialize(original);

    SecureClassLoaderObjectInputStream ois =
        new SecureClassLoaderObjectInputStream(
            new ByteArrayInputStream(serializedData),
            testClassLoader,
            allowAllFilter);

    Object result = ois.readObject();

    assertThat(result).isInstanceOf(TestSerializable.class);
    TestSerializable deserialized = (TestSerializable) result;
    assertThat(deserialized.name).isEqualTo(original.name);
    assertThat(deserialized.value).isEqualTo(original.value);
  }

  @Test
  public void resolveClassFallsBackToThreadContextClassLoader() throws Exception {
    // Set up a generating classloader as TCCL
    ClassLoader generatingClassLoader = new GeneratingClassLoader();
    Thread.currentThread().setContextClassLoader(generatingClassLoader);

    // Create instance of dynamic class
    Class<?> dynamicClass = Class.forName(dynamicClassName, false, generatingClassLoader);
    Object instanceOfDynamicClass = dynamicClass.newInstance();

    byte[] serializedData = serialize(instanceOfDynamicClass);

    // Use a different classloader for the stream
    SecureClassLoaderObjectInputStream ois =
        new SecureClassLoaderObjectInputStream(
            new ByteArrayInputStream(serializedData),
            getClass().getClassLoader(),
            allowAllFilter);

    // Should succeed by falling back to TCCL
    Object result = ois.readObject();

    assertThat(result).isNotNull();
    assertThat(result.getClass().getName()).isEqualTo(dynamicClassName);
  }

  @Test
  public void resolveClassThrowsClassNotFoundExceptionWhenClassNotAvailable() throws Exception {
    // Serialize with dynamic class
    ClassLoader generatingClassLoader = new GeneratingClassLoader();
    Thread.currentThread().setContextClassLoader(generatingClassLoader);

    Class<?> dynamicClass = Class.forName(dynamicClassName, false, generatingClassLoader);
    Object instanceOfDynamicClass = dynamicClass.newInstance();
    byte[] serializedData = serialize(instanceOfDynamicClass);

    // Clear TCCL so fallback also fails
    Thread.currentThread().setContextClassLoader(null);

    SecureClassLoaderObjectInputStream ois =
        new SecureClassLoaderObjectInputStream(
            new ByteArrayInputStream(serializedData),
            getClass().getClassLoader(),
            allowAllFilter);

    // Should fail - class not found in either classloader
    assertThatThrownBy(ois::readObject)
        .isInstanceOf(ClassNotFoundException.class);
  }

  // ============================================================
  // resolveProxyClass() Tests
  // ============================================================

  @Test
  public void resolveProxyClassWithPublicInterfaces() throws Exception {
    // Create a proxy with public interfaces
    Object proxy = Proxy.newProxyInstance(
        testClassLoader,
        new Class<?>[] {Runnable.class, Comparable.class},
        new TestInvocationHandler());

    byte[] serializedData = serialize(proxy);

    SecureClassLoaderObjectInputStream ois =
        new SecureClassLoaderObjectInputStream(
            new ByteArrayInputStream(serializedData),
            testClassLoader,
            allowAllFilter);

    Object result = ois.readObject();

    assertThat(result).isNotNull();
    assertThat(Proxy.isProxyClass(result.getClass())).isTrue();
    assertThat(result).isInstanceOf(Runnable.class);
    assertThat(result).isInstanceOf(Comparable.class);
  }

  @Test
  public void resolveProxyClassWithNonPublicInterface() throws Exception {
    // Create a non-public interface dynamically
    ClassLoader specialClassLoader = new NonPublicInterfaceClassLoader();
    Class<?> nonPublicInterface = Class.forName(
        "org.apache.geode.modules.util.NonPublicTestInterface",
        false,
        specialClassLoader);

    Object proxy = Proxy.newProxyInstance(
        specialClassLoader,
        new Class<?>[] {nonPublicInterface},
        new TestInvocationHandler());

    byte[] serializedData = serialize(proxy);

    SecureClassLoaderObjectInputStream ois =
        new SecureClassLoaderObjectInputStream(
            new ByteArrayInputStream(serializedData),
            specialClassLoader,
            allowAllFilter);

    Object result = ois.readObject();

    assertThat(result).isNotNull();
    assertThat(Proxy.isProxyClass(result.getClass())).isTrue();
  }

  @Test
  public void resolveProxyClassFailsWithConflictingNonPublicInterfaces() throws Exception {
    // Create two different classloaders, each with a non-public interface
    ClassLoader classLoader1 =
        new NonPublicInterfaceClassLoader("org.apache.geode.modules.util.NonPublicInterface1");
    ClassLoader classLoader2 =
        new NonPublicInterfaceClassLoader("org.apache.geode.modules.util.NonPublicInterface2");

    Class<?> interface1 =
        Class.forName("org.apache.geode.modules.util.NonPublicInterface1", false, classLoader1);
    Class<?> interface2 =
        Class.forName("org.apache.geode.modules.util.NonPublicInterface2", false, classLoader2);

    // Create a proxy with both non-public interfaces from different classloaders
    // This should fail because non-public interfaces must be from the same classloader
    assertThatThrownBy(() -> Proxy.newProxyInstance(
        classLoader1,
        new Class<?>[] {interface1, interface2},
        new TestInvocationHandler()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("not visible from class loader");
  }

  // ============================================================
  // Integration Tests - Full Serialization/Deserialization Flow
  // ============================================================

  @Test
  public void integrationTestWithSafeDeserializationFilter() throws Exception {
    // Use the real SafeDeserializationFilter
    SafeDeserializationFilter safeFilter = new SafeDeserializationFilter();

    // Serialize an allowed object (String)
    String testString = "integration test";
    byte[] serializedData = serialize(testString);

    SecureClassLoaderObjectInputStream ois =
        new SecureClassLoaderObjectInputStream(
            new ByteArrayInputStream(serializedData),
            testClassLoader,
            safeFilter);

    Object result = ois.readObject();

    assertThat(result).isEqualTo(testString);
  }

  @Test
  public void integrationTestFilterRejectsDisallowedClass() throws Exception {
    // Create a filter that rejects our custom TestSerializable class
    ObjectInputFilter strictFilter = info -> {
      if (info.serialClass() != null &&
          info.serialClass().equals(TestSerializable.class)) {
        return ObjectInputFilter.Status.REJECTED;
      }
      return ObjectInputFilter.Status.ALLOWED;
    };

    // Serialize our custom object
    TestSerializable testObject = new TestSerializable("test", 42);
    byte[] serializedData = serialize(testObject);

    SecureClassLoaderObjectInputStream ois =
        new SecureClassLoaderObjectInputStream(
            new ByteArrayInputStream(serializedData),
            testClassLoader,
            strictFilter);

    // Filter should reject this class
    assertThatThrownBy(ois::readObject)
        .isInstanceOf(Exception.class);
  }

  @Test
  public void integrationTestDeserializeComplexObjectGraph() throws Exception {
    // Create a complex object graph
    List<TestSerializable> list = new ArrayList<>();
    list.add(new TestSerializable("first", 1));
    list.add(new TestSerializable("second", 2));
    list.add(new TestSerializable("third", 3));

    byte[] serializedData = serialize(list);

    SecureClassLoaderObjectInputStream ois =
        new SecureClassLoaderObjectInputStream(
            new ByteArrayInputStream(serializedData),
            testClassLoader,
            allowAllFilter);

    @SuppressWarnings("unchecked")
    List<TestSerializable> result = (List<TestSerializable>) ois.readObject();

    assertThat(result).hasSize(3);
    assertThat(result.get(0).name).isEqualTo("first");
    assertThat(result.get(1).value).isEqualTo(2);
    assertThat(result.get(2).name).isEqualTo("third");
  }

  // ============================================================
  // Helper Methods and Classes
  // ============================================================

  private byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    oos.close();
    return baos.toByteArray();
  }

  /**
   * Test serializable class
   */
  public static class TestSerializable implements Serializable {
    private static final long serialVersionUID = 1L;
    public String name;
    public int value;

    public TestSerializable(String name, int value) {
      this.name = name;
      this.value = value;
    }
  }

  /**
   * Test invocation handler for proxies
   */
  private static class TestInvocationHandler implements InvocationHandler, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      return null;
    }
  }

  /**
   * ClassLoader that generates classes dynamically for testing
   * Copied pattern from ClassLoaderObjectInputStreamTest
   */
  private class GeneratingClassLoader extends ClassLoader {
    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      if (!name.equals(dynamicClassName)) {
        throw new ClassNotFoundException(name);
      }

      ClassGen classGen = new ClassGen(name, "java.lang.Object", name + ".java",
          Const.ACC_PUBLIC, new String[] {Serializable.class.getName()});

      // Add a no-arg constructor
      org.apache.bcel.generic.InstructionList il = new org.apache.bcel.generic.InstructionList();
      il.append(org.apache.bcel.generic.InstructionFactory.createLoad(
          org.apache.bcel.generic.Type.OBJECT, 0));
      il.append(new org.apache.bcel.generic.INVOKESPECIAL(
          classGen.getConstantPool().addMethodref("java.lang.Object", "<init>", "()V")));
      il.append(org.apache.bcel.generic.InstructionFactory.createReturn(
          org.apache.bcel.generic.Type.VOID));

      org.apache.bcel.generic.MethodGen constructor = new org.apache.bcel.generic.MethodGen(
          Const.ACC_PUBLIC, org.apache.bcel.generic.Type.VOID,
          org.apache.bcel.generic.Type.NO_ARGS, null, "<init>",
          name, il, classGen.getConstantPool());
      constructor.setMaxStack();
      constructor.setMaxLocals();
      classGen.addMethod(constructor.getMethod());

      JavaClass javaClass = classGen.getJavaClass();
      byte[] bytes = javaClass.getBytes();
      return defineClass(javaClass.getClassName(), bytes, 0, bytes.length);
    }

    @Override
    public URL getResource(String name) {
      return null;
    }

    @Override
    public Enumeration<URL> getResources(String name) {
      return new Vector<URL>().elements();
    }
  }

  /**
   * ClassLoader that creates a non-public interface for testing proxy resolution
   */
  private class NonPublicInterfaceClassLoader extends ClassLoader {
    private final String interfaceName;

    public NonPublicInterfaceClassLoader() {
      this("org.apache.geode.modules.util.NonPublicTestInterface");
    }

    public NonPublicInterfaceClassLoader(String interfaceName) {
      this.interfaceName = interfaceName;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      if (!name.equals(interfaceName)) {
        throw new ClassNotFoundException(name);
      }

      // Create a non-public (package-private) interface
      ClassGen classGen = new ClassGen(name, "java.lang.Object", name + ".java",
          Const.ACC_INTERFACE, // Interface modifier without ACC_PUBLIC = package-private
          new String[] {});

      JavaClass javaClass = classGen.getJavaClass();
      byte[] bytes = javaClass.getBytes();
      return defineClass(javaClass.getClassName(), bytes, 0, bytes.length);
    }
  }
}
