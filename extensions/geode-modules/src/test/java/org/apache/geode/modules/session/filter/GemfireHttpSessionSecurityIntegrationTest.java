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
package org.apache.geode.modules.session.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.modules.util.SecureClassLoaderObjectInputStream;

/**
 * Integration tests for session attribute deserialization with security filtering.
 *
 * These tests verify the end-to-end flow of deserializing session attributes
 * with malicious payload detection, simulating the getAttribute() security path.
 */
public class GemfireHttpSessionSecurityIntegrationTest {

  private ClassLoader testClassLoader;
  private SafeDeserializationFilter securityFilter;

  @Before
  public void setUp() {
    testClassLoader = getClass().getClassLoader();
    securityFilter = new SafeDeserializationFilter();
  }

  // ============================================================
  // Integration Tests - Simulate getAttribute() Security Flow
  // ============================================================

  @Test
  public void deserializeAllowedClassSucceeds() throws Exception {
    // Simulate the security flow in getAttribute() with an allowed class
    String testData = "safe session data";
    byte[] serialized = serialize(testData);

    // This simulates what happens in getAttribute() when classloader differs
    SecureClassLoaderObjectInputStream ois = new SecureClassLoaderObjectInputStream(
        new ByteArrayInputStream(serialized),
        testClassLoader,
        securityFilter);

    Object result = ois.readObject();

    assertThat(result).isEqualTo(testData);
    assertThat(result).isInstanceOf(String.class);
  }

  @Test
  public void deserializeBlockedClassThrowsSecurityException() throws Exception {
    // Simulate attack: attempt to deserialize a dangerous class
    DangerousGadgetClass malicious = new DangerousGadgetClass();
    byte[] serialized = serialize(malicious);

    SecureClassLoaderObjectInputStream ois = new SecureClassLoaderObjectInputStream(
        new ByteArrayInputStream(serialized),
        testClassLoader,
        securityFilter);

    // SafeDeserializationFilter should reject this class
    assertThatThrownBy(ois::readObject)
        .isInstanceOf(Exception.class); // InvalidClassException or SecurityException
  }

  @Test
  public void deserializeComplexAllowedObjectGraph() throws Exception {
    // Test with a list of strings (simple but complex structure)
    ArrayList<String> complexData = new ArrayList<>();
    complexData.add("item1");
    complexData.add("item2");
    complexData.add("item3");

    byte[] serialized = serialize(complexData);

    SecureClassLoaderObjectInputStream ois = new SecureClassLoaderObjectInputStream(
        new ByteArrayInputStream(serialized),
        testClassLoader,
        securityFilter);

    @SuppressWarnings("unchecked")
    ArrayList<String> result = (ArrayList<String>) ois.readObject();

    assertThat(result).isNotNull();
    assertThat(result).hasSize(3);
    assertThat(result.get(0)).isEqualTo("item1");
  }

  @Test
  public void deserializeWithMultipleAllowedTypes() throws Exception {
    // Test with Integer (another common allowed type)
    Integer value = Integer.valueOf(42);

    byte[] serialized = serialize(value);

    SecureClassLoaderObjectInputStream ois = new SecureClassLoaderObjectInputStream(
        new ByteArrayInputStream(serialized),
        testClassLoader,
        securityFilter);

    Integer result = (Integer) ois.readObject();

    assertThat(result).isEqualTo(42);
  }

  @Test
  public void deserializeNestedBlockedClassThrowsException() throws Exception {
    // Test that blocked classes are detected even when nested
    HashMap<String, Object> container = new HashMap<>();
    container.put("safe", "data");
    container.put("malicious", new DangerousGadgetClass());

    byte[] serialized = serialize(container);

    SecureClassLoaderObjectInputStream ois = new SecureClassLoaderObjectInputStream(
        new ByteArrayInputStream(serialized),
        testClassLoader,
        securityFilter);

    // Should be rejected even though the container (HashMap) is allowed
    assertThatThrownBy(ois::readObject)
        .isInstanceOf(Exception.class);
  }

  @Test
  public void deserializeWithCustomAllowedClass() throws Exception {
    // Test that we can add custom classes to the allowlist
    SafeDeserializationFilter customFilter = SafeDeserializationFilter.createWithAllowedClasses(
        AllowedCustomClass.class.getName());

    AllowedCustomClass custom = new AllowedCustomClass("custom data");
    byte[] serialized = serialize(custom);

    SecureClassLoaderObjectInputStream ois = new SecureClassLoaderObjectInputStream(
        new ByteArrayInputStream(serialized),
        testClassLoader,
        customFilter);

    AllowedCustomClass result = (AllowedCustomClass) ois.readObject();

    assertThat(result.data).isEqualTo("custom data");
  }

  @Test
  public void deserializeAfterSecurityExceptionLeavesStreamInSafeState() throws Exception {
    // Verify that after a security exception, the stream doesn't leave resources in bad state
    DangerousGadgetClass malicious = new DangerousGadgetClass();
    byte[] serialized = serialize(malicious);

    SecureClassLoaderObjectInputStream ois = new SecureClassLoaderObjectInputStream(
        new ByteArrayInputStream(serialized),
        testClassLoader,
        securityFilter);

    try {
      ois.readObject();
    } catch (Exception e) {
      // Expected - security filter rejection
    }

    // Stream should be closeable without errors
    ois.close(); // Should not throw
  }

  @Test
  public void deserializeVerifiesFilterIsActiveFromStart() throws Exception {
    // Verify that the filter is enforced immediately, not after first readObject()
    byte[] serialized = serialize("test");

    SecureClassLoaderObjectInputStream ois = new SecureClassLoaderObjectInputStream(
        new ByteArrayInputStream(serialized),
        testClassLoader,
        securityFilter);

    // Filter should be set
    assertThat(ois.getFilter()).isEqualTo(securityFilter);

    // And should work for deserialization
    Object result = ois.readObject();
    assertThat(result).isEqualTo("test");
  }

  // ============================================================
  // Helper Methods
  // ============================================================

  private byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    oos.close();
    return baos.toByteArray();
  }

  // ============================================================
  // Test Classes
  // ============================================================

  /**
   * Container with various allowed JDK types
   */
  public static class AllowedTypesContainer implements Serializable {
    private static final long serialVersionUID = 1L;
    public String stringValue;
    public int intValue;
    public List<String> listValue;
    public HashMap<String, String> mapValue;
  }

  /**
   * Custom class that can be added to allowlist
   */
  public static class AllowedCustomClass implements Serializable {
    private static final long serialVersionUID = 1L;
    public String data;

    public AllowedCustomClass(String data) {
      this.data = data;
    }
  }

  /**
   * Dangerous class simulating a gadget chain component.
   * This represents attack classes like CommonsCollections InvokerTransformer
   * that are used in real deserialization exploits.
   *
   * IMPORTANT: This is a SAFE test class. It does NOT execute malicious code.
   * It only simulates what a real exploit class would look like.
   *
   * The SafeDeserializationFilter should REJECT this class during deserialization,
   * preventing readObject() from ever being called.
   */
  public static class DangerousGadgetClass implements Serializable {
    private static final long serialVersionUID = 1L;
    public String exploitPayload = "malicious code";

    /**
     * Custom readObject() method - this is what makes gadget chains dangerous.
     *
     * In REAL exploits, this method would contain:
     * - Runtime.getRuntime().exec("malicious command")
     * - JNDI lookups to remote servers
     * - Reflection-based attacks
     *
     * In THIS TEST, the method intentionally does nothing malicious.
     * We're verifying that SafeDeserializationFilter blocks the class
     * BEFORE this method can execute.
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      // Intentionally empty - this is a safe test class
      // Real gadgets would execute malicious code here
    }
  }
}
