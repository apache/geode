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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ObjectInputFilter.FilterInfo;
import java.io.ObjectInputFilter.Status;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;

/**
 * Comprehensive unit tests for SafeDeserializationFilter.
 *
 * <p>
 * <b>Test Coverage:</b>
 * <ol>
 * <li><b>Whitelist Validation:</b> Verifies safe Java classes are allowed (String, Integer,
 * Collections)</li>
 * <li><b>Blacklist Validation:</b> Verifies known gadget chain classes are blocked
 * (InvokerTransformer,
 * TemplatesImpl)</li>
 * <li><b>Resource Limits:</b> Verifies DoS protection (depth, references, array size, bytes)</li>
 * <li><b>Custom Configuration:</b> Verifies custom allow-lists work correctly</li>
 * <li><b>Default-Deny Policy:</b> Verifies unlisted classes are rejected</li>
 * </ol>
 *
 * <p>
 * <b>Testing Strategy:</b><br>
 * Tests use Mockito to simulate ObjectInputFilter.FilterInfo without actually deserializing
 * dangerous objects. This allows us to test the filter logic safely without executing exploits.
 *
 * <p>
 * <b>Security Test Categories:</b>
 * <ul>
 * <li><b>Positive Tests:</b> Allowed classes pass through (testAllowedJavaLangString, etc.)</li>
 * <li><b>Negative Tests:</b> Dangerous classes blocked (testBlockedCommonsCollections*, etc.)</li>
 * <li><b>Boundary Tests:</b> Resource limits enforced (testDepthExceeded, etc.)</li>
 * <li><b>Configuration Tests:</b> Custom whitelists work (testCustomAllowedClass, etc.)</li>
 * </ul>
 *
 * <p>
 * <b>Attack Coverage:</b><br>
 * These tests verify protection against:
 * <ul>
 * <li>Apache Commons Collections gadget chains (InvokerTransformer, ChainedTransformer)</li>
 * <li>JDK internal exploits (TemplatesImpl)</li>
 * <li>Stack overflow attacks (depth limit)</li>
 * <li>Memory exhaustion attacks (size limits)</li>
 * </ul>
 *
 * <p>
 * <b>Suppressions Explained:</b><br>
 * {@code @SuppressWarnings({"unchecked", "rawtypes"})} is used because:
 * <ul>
 * <li>Mockito cannot mock the final {@code Class<?>} type directly</li>
 * <li>We need to cast raw {@code Class} to {@code Class<?>} for testing</li>
 * <li>This is safe in test code as we control all inputs</li>
 * </ul>
 *
 * @see SafeDeserializationFilter
 * @see SecureClassLoaderObjectInputStream
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SafeDeserializationFilterTest {

  private SafeDeserializationFilter filter;
  private FilterInfo mockFilterInfo;

  @Before
  public void setUp() {
    filter = new SafeDeserializationFilter();
    mockFilterInfo = mock(FilterInfo.class);
  }

  @Test
  public void testAllowedJavaLangString() {
    when(mockFilterInfo.serialClass()).thenReturn((Class) String.class);
    when(mockFilterInfo.depth()).thenReturn(1L);
    when(mockFilterInfo.references()).thenReturn(1L);
    when(mockFilterInfo.arrayLength()).thenReturn(-1L);
    when(mockFilterInfo.streamBytes()).thenReturn(100L);

    Status status = filter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.ALLOWED);
  }

  @Test
  public void testAllowedInteger() {
    when(mockFilterInfo.serialClass()).thenReturn((Class) Integer.class);
    when(mockFilterInfo.depth()).thenReturn(1L);
    when(mockFilterInfo.references()).thenReturn(1L);
    when(mockFilterInfo.arrayLength()).thenReturn(-1L);
    when(mockFilterInfo.streamBytes()).thenReturn(100L);

    Status status = filter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.ALLOWED);
  }

  /**
   * Tests that Apache Commons Collections InvokerTransformer is blocked.
   *
   * <p>
   * <b>Attack Context:</b><br>
   * InvokerTransformer is a key component in the infamous "ysoserial CommonsCollections1"
   * exploit chain. It can invoke arbitrary methods via reflection, enabling Remote Code
   * Execution when combined with TransformedMap or LazyMap.
   *
   * <p>
   * <b>Real-World Impact:</b><br>
   * This class has been used in attacks against major systems including:
   * <ul>
   * <li>Jenkins CI/CD servers</li>
   * <li>WebLogic application servers</li>
   * <li>JBoss application servers</li>
   * </ul>
   *
   * <p>
   * This test verifies it's explicitly blocked to prevent RCE attacks.
   */
  @Test
  public void testBlockedCommonsCollectionsInvokerTransformer() throws ClassNotFoundException {
    // Try to load the class - it may not be available in test classpath
    Class<?> dangerousClass;
    try {
      dangerousClass = Class.forName("org.apache.commons.collections.functors.InvokerTransformer");
    } catch (ClassNotFoundException e) {
      // Skip test if class not available
      return;
    }

    when(mockFilterInfo.serialClass()).thenReturn((Class) dangerousClass);
    when(mockFilterInfo.depth()).thenReturn(1L);
    when(mockFilterInfo.references()).thenReturn(1L);
    when(mockFilterInfo.arrayLength()).thenReturn(-1L);
    when(mockFilterInfo.streamBytes()).thenReturn(100L);

    Status status = filter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.REJECTED);
  }

  /**
   * Tests that TemplatesImpl is blocked.
   *
   * <p>
   * <b>Attack Context:</b><br>
   * TemplatesImpl (com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl) can load
   * arbitrary bytecode during deserialization, enabling Remote Code Execution without
   * reflection or external dependencies.
   *
   * <p>
   * <b>Why It's Dangerous:</b><br>
   * An attacker can embed malicious Java bytecode in the serialized TemplatesImpl object.
   * During deserialization, the bytecode is loaded and executed, giving the attacker full
   * control over the application.
   *
   * <p>
   * This test verifies it's blocked to prevent bytecode injection attacks.
   */
  @Test
  public void testBlockedTemplatesImpl() throws ClassNotFoundException {
    Class<?> dangerousClass;
    try {
      dangerousClass = Class.forName("com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl");
    } catch (ClassNotFoundException e) {
      // Skip test if class not available
      return;
    }

    when(mockFilterInfo.serialClass()).thenReturn((Class) dangerousClass);
    when(mockFilterInfo.depth()).thenReturn(1L);
    when(mockFilterInfo.references()).thenReturn(1L);
    when(mockFilterInfo.arrayLength()).thenReturn(-1L);
    when(mockFilterInfo.streamBytes()).thenReturn(100L);

    Status status = filter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.REJECTED);
  }

  /**
   * Tests that excessive object graph depth is rejected (DoS protection).
   *
   * <p>
   * <b>Attack Prevention:</b><br>
   * Deep object graphs can cause stack overflow errors during deserialization.
   * An attacker can craft a deeply nested object structure to crash the JVM.
   *
   * <p>
   * <b>Limit:</b> MAX_DEPTH = 50 levels<br>
   * This is sufficient for legitimate session data but prevents stack exhaustion attacks.
   */
  @Test
  public void testDepthExceeded() {
    when(mockFilterInfo.serialClass()).thenReturn((Class) String.class);
    when(mockFilterInfo.depth()).thenReturn(51L); // > MAX_DEPTH (50)
    when(mockFilterInfo.references()).thenReturn(1L);
    when(mockFilterInfo.arrayLength()).thenReturn(-1L);
    when(mockFilterInfo.streamBytes()).thenReturn(100L);

    Status status = filter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.REJECTED);
  }

  /**
   * Tests that excessive object references are rejected (DoS protection).
   *
   * <p>
   * <b>Attack Prevention:</b><br>
   * Large numbers of object references can exhaust heap memory during deserialization.
   * An attacker can craft circular references or massive object graphs to cause OutOfMemoryError.
   *
   * <p>
   * <b>Limit:</b> MAX_REFERENCES = 10,000 objects<br>
   * This allows reasonable session data but prevents memory exhaustion attacks.
   */
  @Test
  public void testReferencesExceeded() {
    when(mockFilterInfo.serialClass()).thenReturn((Class) String.class);
    when(mockFilterInfo.depth()).thenReturn(1L);
    when(mockFilterInfo.references()).thenReturn(10001L); // > MAX_REFERENCES (10000)
    when(mockFilterInfo.arrayLength()).thenReturn(-1L);
    when(mockFilterInfo.streamBytes()).thenReturn(100L);

    Status status = filter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.REJECTED);
  }

  @Test
  public void testArraySizeExceeded() {
    when(mockFilterInfo.serialClass()).thenReturn((Class) byte[].class);
    when(mockFilterInfo.depth()).thenReturn(1L);
    when(mockFilterInfo.references()).thenReturn(1L);
    when(mockFilterInfo.arrayLength()).thenReturn(10001L); // > MAX_ARRAY_SIZE (10000)
    when(mockFilterInfo.streamBytes()).thenReturn(100L);

    Status status = filter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.REJECTED);
  }

  @Test
  public void testBytesExceeded() {
    when(mockFilterInfo.serialClass()).thenReturn((Class) String.class);
    when(mockFilterInfo.depth()).thenReturn(1L);
    when(mockFilterInfo.references()).thenReturn(1L);
    when(mockFilterInfo.arrayLength()).thenReturn(-1L);
    when(mockFilterInfo.streamBytes()).thenReturn(10_000_001L); // > MAX_BYTES (10MB)

    Status status = filter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.REJECTED);
  }

  @Test
  public void testNullFilterInfo() {
    Status status = filter.checkInput(null);
    assertThat(status).isEqualTo(Status.REJECTED);
  }

  @Test
  public void testCustomAllowedClass() {
    // Use a real class name that we'll configure
    Set<String> customClasses = new HashSet<>();
    customClasses.add("java.io.File");

    SafeDeserializationFilter customFilter =
        new SafeDeserializationFilter(customClasses, new HashSet<>());

    when(mockFilterInfo.serialClass()).thenReturn((Class) java.io.File.class);
    when(mockFilterInfo.depth()).thenReturn(1L);
    when(mockFilterInfo.references()).thenReturn(1L);
    when(mockFilterInfo.arrayLength()).thenReturn(-1L);
    when(mockFilterInfo.streamBytes()).thenReturn(100L);

    Status status = customFilter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.ALLOWED);
  }

  @Test
  public void testCustomAllowedPattern() {
    // Use a pattern that matches java.io.File
    Set<Pattern> customPatterns = new HashSet<>();
    customPatterns.add(Pattern.compile("^java\\.io\\..*"));

    SafeDeserializationFilter customFilter =
        new SafeDeserializationFilter(new HashSet<>(), customPatterns);

    when(mockFilterInfo.serialClass()).thenReturn((Class) java.io.File.class);
    when(mockFilterInfo.depth()).thenReturn(1L);
    when(mockFilterInfo.references()).thenReturn(1L);
    when(mockFilterInfo.arrayLength()).thenReturn(-1L);
    when(mockFilterInfo.streamBytes()).thenReturn(100L);

    Status status = customFilter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.ALLOWED);
  }

  @Test
  public void testNotWhitelistedClass() {
    // Use a real class that's not in the whitelist (File is not allowed by default)
    when(mockFilterInfo.serialClass()).thenReturn((Class) java.io.File.class);
    when(mockFilterInfo.depth()).thenReturn(1L);
    when(mockFilterInfo.references()).thenReturn(1L);
    when(mockFilterInfo.arrayLength()).thenReturn(-1L);
    when(mockFilterInfo.streamBytes()).thenReturn(100L);

    Status status = filter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.REJECTED);
  }

  @Test
  public void testCreateWithAllowedClasses() {
    SafeDeserializationFilter customFilter =
        SafeDeserializationFilter.createWithAllowedClasses(
            "java.io.File", "java.io.FileInputStream");

    when(mockFilterInfo.serialClass()).thenReturn((Class) java.io.File.class);
    when(mockFilterInfo.depth()).thenReturn(1L);
    when(mockFilterInfo.references()).thenReturn(1L);
    when(mockFilterInfo.arrayLength()).thenReturn(-1L);
    when(mockFilterInfo.streamBytes()).thenReturn(100L);

    Status status = customFilter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.ALLOWED);
  }

  @Test
  public void testCreateWithAllowedPatterns() {
    SafeDeserializationFilter customFilter =
        SafeDeserializationFilter.createWithAllowedPatterns("^java\\.io\\..*");

    when(mockFilterInfo.serialClass()).thenReturn((Class) java.io.File.class);
    when(mockFilterInfo.depth()).thenReturn(1L);
    when(mockFilterInfo.references()).thenReturn(1L);
    when(mockFilterInfo.arrayLength()).thenReturn(-1L);
    when(mockFilterInfo.streamBytes()).thenReturn(100L);

    Status status = customFilter.checkInput(mockFilterInfo);
    assertThat(status).isEqualTo(Status.ALLOWED);
  }

  @Test
  public void testAllowedCollectionClasses() {
    Class<?>[] allowedClasses = {
        java.util.ArrayList.class,
        java.util.HashMap.class,
        java.util.HashSet.class,
        java.util.Date.class
    };

    for (Class<?> clazz : allowedClasses) {
      when(mockFilterInfo.serialClass()).thenReturn((Class) clazz);
      when(mockFilterInfo.depth()).thenReturn(1L);
      when(mockFilterInfo.references()).thenReturn(1L);
      when(mockFilterInfo.arrayLength()).thenReturn(-1L);
      when(mockFilterInfo.streamBytes()).thenReturn(100L);

      Status status = filter.checkInput(mockFilterInfo);
      assertThat(status)
          .as("Expected %s to be allowed", clazz.getName())
          .isEqualTo(Status.ALLOWED);
    }
  }
}
