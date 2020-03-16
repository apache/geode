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
package org.apache.geode.management.internal.cli.util;

import static org.apache.geode.internal.Assert.assertNotNull;

import java.io.FileNotFoundException;
import java.util.EmptyStackException;
import java.util.Stack;

import org.apache.commons.lang3.JavaVersion;

import org.apache.geode.GemFireException;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.internal.i18n.CliStrings;

public class JdkTool {
  protected static final String JAVA_HOME = System.getProperty("java.home");

  public static String getJVisualVMPathname() {
    if (SystemUtils.isMacOSX()) {
      try {
        return IOUtils.verifyPathnameExists(
            "/System/Library/Java/Support/VisualVM.bundle/Contents/Home/bin/jvisualvm");
      } catch (FileNotFoundException e) {
        throw new VisualVmNotFoundException(CliStrings.START_JVISUALVM__NOT_FOUND_ERROR_MESSAGE, e);
      }
    } else { // Linux, Solaris, Windows, etc...
      try {
        return getJdkToolPathname("jvisualvm" + getExecutableSuffix(),
            new VisualVmNotFoundException(CliStrings.START_JVISUALVM__NOT_FOUND_ERROR_MESSAGE));
      } catch (VisualVmNotFoundException e) {
        if (!org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_1_6)) {
          throw new VisualVmNotFoundException(
              CliStrings.START_JVISUALVM__EXPECTED_JDK_VERSION_ERROR_MESSAGE);
        }

        throw e;
      }
    }
  }

  public static String getJConsolePathname() {
    return getJdkToolPathname("jconsole" + getExecutableSuffix(),
        new JConsoleNotFoundException(CliStrings.START_JCONSOLE__NOT_FOUND_ERROR_MESSAGE));
  }

  protected static String getJdkToolPathname(final String jdkToolExecutableName,
      final GemFireException throwable) {
    assertNotNull(jdkToolExecutableName, "The JDK tool executable name cannot be null!");
    assertNotNull(throwable, "The GemFireException cannot be null!");

    Stack<String> pathnames = new Stack<>();

    pathnames.push(jdkToolExecutableName);
    pathnames
        .push(IOUtils.appendToPath(System.getenv("JAVA_HOME"), "..", "bin", jdkToolExecutableName));
    pathnames.push(IOUtils.appendToPath(System.getenv("JAVA_HOME"), "bin", jdkToolExecutableName));
    pathnames.push(IOUtils.appendToPath(JAVA_HOME, "..", "bin", jdkToolExecutableName));
    pathnames.push(IOUtils.appendToPath(JAVA_HOME, "bin", jdkToolExecutableName));

    return getJdkToolPathname(pathnames, throwable);
  }

  protected static String getJdkToolPathname(final Stack<String> pathnames,
      final GemFireException throwable) {
    assertNotNull(pathnames, "The JDK tool executable pathnames cannot be null!");
    assertNotNull(throwable, "The GemFireException cannot be null!");

    try {
      // assume 'java.home' JVM System property refers to the JDK installation directory. note,
      // however, that the
      // 'java.home' JVM System property usually refers to the JRE used to launch this application
      return IOUtils.verifyPathnameExists(pathnames.pop());
    } catch (EmptyStackException ignore) {
      throw throwable;
    } catch (FileNotFoundException ignore) {
      return getJdkToolPathname(pathnames, throwable);
    }
  }

  protected static String getExecutableSuffix() {
    return SystemUtils.isWindows() ? ".exe" : "";
  }
}
