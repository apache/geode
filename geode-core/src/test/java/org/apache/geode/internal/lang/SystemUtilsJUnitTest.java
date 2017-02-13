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
package org.apache.geode.internal.lang;

import static org.apache.geode.internal.lang.SystemUtils.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.lang.management.ManagementFactory;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * The SystemUtilsJUnitTest class is a test suite of test cases for testing the contract and
 * functionality of the SystemUtils class.
 * <p/>
 * 
 * @see org.apache.geode.internal.lang.SystemUtils
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 6.8
 */
@Category(UnitTest.class)
public class SystemUtilsJUnitTest {

  // NOTE this test adds some maintenance overhead but ensure the correct functioning of GemFire
  // code that relies on
  // isJavaVersionAtLeast
  @Test
  public void testIsJavaVersionAtLeast() {
    // note, the expected version value should be set to the minimum supported version of the Java
    // Runtime Environment
    // (JRE) for GemFire
    assertTrue(isJavaVersionAtLeast("1.8"));
    // note, the expected version value should be set to the next version of the Java Runtime
    // Environment (JRE)
    // not currently available.
    assertFalse(isJavaVersionAtLeast("1.9"));
  }

  @Test
  public void testIsAppleJVM() {
    final boolean expected =
        ManagementFactory.getRuntimeMXBean().getVmVendor().contains(APPLE_JVM_VENDOR_NAME);
    assertEquals(expected, isAppleJVM());
  }

  @Test
  public void testIsOracleJVM() {
    final boolean expected =
        ManagementFactory.getRuntimeMXBean().getVmVendor().contains(ORACLE_JVM_VENDOR_NAME);
    assertEquals(expected, isOracleJVM());
  }

  @Test
  public void testIsHotSpotVM() {
    final boolean expected =
        ManagementFactory.getRuntimeMXBean().getVmName().contains(JAVA_HOTSPOT_JVM_NAME);
    assertEquals(expected, isHotSpotVM());
  }

  @Test
  public void testIsJ9VM() {
    final boolean expected =
        ManagementFactory.getRuntimeMXBean().getVmName().contains(IBM_J9_JVM_NAME);
    assertEquals(expected, isJ9VM());
  }

  @Test
  public void testIsJRockitVM() {
    final boolean expected =
        ManagementFactory.getRuntimeMXBean().getVmName().contains(ORACLE_JROCKIT_JVM_NAME);
    assertEquals(expected, isJRockitVM());
  }

  @Test
  public void testIsLinux() {
    final boolean expected =
        ManagementFactory.getOperatingSystemMXBean().getName().contains(LINUX_OS_NAME);
    assertEquals(expected, isLinux());
  }

  @Test
  public void testIsMacOSX() {
    final boolean expected =
        ManagementFactory.getOperatingSystemMXBean().getName().contains(MAC_OSX_NAME);
    assertEquals(expected, isMacOSX());
  }

  @Test
  public void testIsWindows() throws Exception {
    final boolean expected =
        ManagementFactory.getOperatingSystemMXBean().getName().contains(WINDOWS_OS_NAME);
    assertEquals(expected, isWindows());
  }

  @Test
  public void getOsNameShouldReturnOsNameValue() {
    assertThat(getOsName()).isEqualTo(System.getProperty("os.name"));
  }

  @Test
  public void getOsVersionShouldReturnOsVersionValue() {
    assertThat(getOsVersion()).isEqualTo(System.getProperty("os.version"));
  }

  @Test
  public void getOsArchitectureShouldReturnOsArchValue() {
    assertThat(getOsArchitecture()).isEqualTo(System.getProperty("os.arch"));
  }

  @Test
  public void getClassPathShouldReturnJavaClassPathValue() {
    assertThat(getClassPath()).isEqualTo(System.getProperty("java.class.path"));
  }

  @Test
  public void getBootClassPathShouldReturnSunBootClassPathValue() {
    String value = System.getProperty("sun.boot.class.path");
    assumeNotNull(value);
    assertThat(getBootClassPath()).isEqualTo(value);
  }

}
