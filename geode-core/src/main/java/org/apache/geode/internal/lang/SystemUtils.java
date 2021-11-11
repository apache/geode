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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.StringTokenizer;

/**
 * The SystemUtils class is an abstract utility class for working with, invoking methods and
 * accessing properties of the Java System class.
 *
 * @see java.lang.System
 * @since GemFire 6.8
 */
@SuppressWarnings("unused")
public class SystemUtils {

  public static final String CURRENT_DIRECTORY = System.getProperty("user.dir");

  // Java Virtual Machine (JVM) Names
  public static final String IBM_J9_JVM_NAME = "J9";
  public static final String JAVA_HOTSPOT_JVM_NAME = "HotSpot";
  public static final String ORACLE_JROCKIT_JVM_NAME = "JRockit";

  // Java Virtual Machine (JVM) Vendor Names
  public static final String APPLE_JVM_VENDOR_NAME = "Apple";
  public static final String IBM_JVM_NAME = "IBM";
  public static final String ORACLE_JVM_VENDOR_NAME = "Oracle";
  public static final String AZUL_JVM_VENDOR_NAME = "Azul";

  // Operating System Names
  public static final String LINUX_OS_NAME = "Linux";
  public static final String MAC_OSX_NAME = "Mac";
  public static final String WINDOWS_OS_NAME = "Windows";
  public static final String SOLARIS_OS_NAME = "SunOS";

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  /**
   * Utility method to determine whether the Java application process is executing on the Apple JVM.
   *
   * @return a boolean value indicating whether the Java application process is executing and
   *         running on the Apple JVM.
   * @see #isJvmVendor(String)
   */
  public static boolean isAppleJVM() {
    return isJvmVendor(APPLE_JVM_VENDOR_NAME);
  }

  /**
   * Utility method to determine whether the Java application process is executing on the Oracle
   * JVM.
   *
   * @return a boolean value indicating whether the Java application process is executing and
   *         running on the Oracle JVM.
   * @see #isJvmVendor(String)
   */
  public static boolean isOracleJVM() {
    return isJvmVendor(ORACLE_JVM_VENDOR_NAME);
  }

  /**
   * Utility method to determine whether the Java application process is executing on the Azul JVM.
   *
   * @return a boolean value indicating whether the Java application process is executing and
   *         running on the Azul JVM.
   * @see #isJvmVendor(String)
   */
  public static boolean isAzulJVM() {
    return isJvmVendor(AZUL_JVM_VENDOR_NAME);
  }

  // @see java.lang.System#getProperty(String) with 'java.vm.vendor'.
  private static boolean isJvmVendor(final String expectedJvmVendorName) {
    String jvmVendor = System.getProperty("java.vm.vendor");
    return jvmVendor != null && jvmVendor.contains(expectedJvmVendorName);
  }

  /**
   * Utility method to determine whether the Java application process is executing on the Java
   * HotSpot VM. Client or Server VM does not matter.
   *
   * @return a boolean value indicating whether the Java application process is executing on the
   *         Java HotSpot VM.
   * @see #isJVM(String)
   */
  public static boolean isHotSpotVM() {
    return isJVM(JAVA_HOTSPOT_JVM_NAME);
  }

  /**
   * Utility method to determine whether the Java application process is executing on the IBM J9 VM.
   *
   * @return a boolean value indicating whether the Java application process is executing on the IBM
   *         J9 VM.
   * @see #isJVM(String)
   */
  public static boolean isJ9VM() {
    return isJVM(IBM_J9_JVM_NAME);
  }

  /**
   * Utility method to determine whether the Java application process is executing on the Oracle
   * JRockit VM. Client or Server VM does not matter.
   *
   * @return a boolean value indicating whether the Java application process is executing on the
   *         Oracle JRockit VM.
   * @see #isJVM(String)
   */
  public static boolean isJRockitVM() {
    return isJVM(ORACLE_JROCKIT_JVM_NAME);
  }

  /**
   * Returns true if the expectedJvmName matches {@code System.getProperty("java.vm.name")}.
   */
  private static boolean isJVM(final String expectedJvmName) {
    String jvmName = System.getProperty("java.vm.name");
    return jvmName != null && jvmName.contains(expectedJvmName);
  }

  /**
   * Utility method that determines whether the Java application process is executing in a Linux
   * operating system environment.
   *
   * @return a boolean value indicating whether the Java application process is executing in Linux.
   * @see #isOS(String)
   */
  public static boolean isLinux() {
    return isOS(LINUX_OS_NAME);
  }

  /**
   * Utility method that determines whether the Java application process is executing in a Apple Mac
   * OSX operating system environment.
   *
   * @return a boolean value indicating whether the Java application process is executing in Mac
   *         OSX.
   * @see #isOS(String)
   */
  public static boolean isMacOSX() {
    return isOS(MAC_OSX_NAME);
  }

  /**
   * Utility method that determines whether the Java application process is executing in a Microsoft
   * Windows-based operating system environment.
   *
   * @return a boolean value indicating whether the Java application process is executing in
   *         Windows.
   * @see #isOS(String)
   */
  public static boolean isWindows() {
    return isOS(WINDOWS_OS_NAME);
  }

  /**
   * Utility method that determines whether the Java application process is executing in a Sun
   * Solaris operating system environment.
   *
   * @return a boolean value indicating whether the Java application process is executing in
   *         Solaris.
   * @see #isOS(String)
   */
  public static boolean isSolaris() {
    return isOS(SOLARIS_OS_NAME);
  }

  /**
   * Returns true if the specified location is in the JVM classpath. This may ignore additions to
   * the classpath that are not reflected by the value in
   * {@code System.getProperty("java.class.path")}.
   *
   * @param location the directory or jar name to test for
   * @return true if location is in the JVM classpath
   */
  public static boolean isInClassPath(String location) throws MalformedURLException {
    return isInClassPath(new File(location).toURI().toURL());
  }

  /**
   * Returns true if the specified location is in the JVM classpath. This may ignore additions to
   * the classpath that are not reflected by the value in
   * {@code System.getProperty("java.class.path")}.
   *
   * @param location the directory or jar URL to test for
   * @return true if location is in the JVM classpath
   */
  public static boolean isInClassPath(URL location) throws MalformedURLException {
    String classPath = getClassPath();
    StringTokenizer st = new StringTokenizer(classPath, File.pathSeparator);
    while (st.hasMoreTokens()) {
      String path = st.nextToken();
      if (location.equals(new File(path).toURI().toURL())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the value of {@code System.getProperty("os.name")}.
   */
  public static String getOsName() {
    return System.getProperty("os.name");
  }

  /**
   * Returns the value of {@code System.getProperty("os.version")}.
   */
  public static String getOsVersion() {
    return System.getProperty("os.version");
  }

  /**
   * Returns the value of {@code System.getProperty("os.arch")}.
   */
  public static String getOsArchitecture() {
    return System.getProperty("os.arch");
  }

  /**
   * Returns the value of {@code System.getProperty("java.class.path")}.
   */
  public static String getClassPath() {
    return System.getProperty("java.class.path");
  }

  /**
   * Returns the value of {@code System.getProperty("sun.boot.class.path")}.
   */
  public static String getBootClassPath() {
    return System.getProperty("sun.boot.class.path");
  }

  /**
   * Returns true if expectedOsName matches {@code System.getProperty("os.name")}.
   */
  private static boolean isOS(final String expectedOsName) {
    String osName = getOsName();
    return osName != null && osName.contains(expectedOsName);
  }

  /**
   * Returns the value of {@code System.getProperty("line.separator")}.
   */
  public static String getLineSeparator() {
    return LINE_SEPARATOR;
  }

}
