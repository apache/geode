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
package org.apache.geode.internal.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.geode.util.internal.GeodeGlossary;

/**
 * A utility class for building up arguments used in spawning another VM
 **/
public class JavaCommandBuilder {

  /**
   * Builds a command line containing all basic arguments required by java
   *
   * @return cmdVec - The caller can then add additional arguments
   */
  public static List<String> buildCommand(final String className, final String additionalClasspath,
      final Properties systemProperties, final List<String> jvmOptions) {
    final List<String> javaCommandLine = new ArrayList<>();

    final File javaBinDir = new File(System.getProperty("java.home"), "bin");
    final File javaCommand = new File(javaBinDir, "java");

    javaCommandLine.add(javaCommand.getPath());

    final String dashServerArg = getDashServerArg(javaBinDir);

    if (dashServerArg != null) {
      javaCommandLine.add(dashServerArg);
    }

    if (jvmOptions != null) {
      for (final String jvmOption : jvmOptions) {
        javaCommandLine.add(jvmOption);
      }
    }

    javaCommandLine.add("-classpath");
    javaCommandLine.add(buildClasspath(additionalClasspath));

    if (systemProperties != null) {
      for (final Object key : systemProperties.keySet()) {
        javaCommandLine.add("-D" + key + "=" + systemProperties.getProperty(key.toString()));
      }
    }

    javaCommandLine.add(className);

    return javaCommandLine;
  }

  private static String buildClasspath(final String additionalClasspath) {
    final StringBuilder classpath = new StringBuilder(System.getProperty("java.class.path"));

    if (additionalClasspath != null) {
      classpath.append(File.pathSeparator);
      classpath.append(additionalClasspath);
    }

    return classpath.toString();
  }

  private static String getDashServerArg(final File javaBinDir) {
    // the gemfire.vmarg.dashserver property allows customers to add a custom argument in place of
    // -server
    final String altDashServerArg =
        System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "vmarg.dashserver", null);
    return (altDashServerArg != null ? altDashServerArg
        : (omitDashServerArg(javaBinDir) ? null : "-server"));
  }

  /**
   * Determine if the -server argument should be omitted for this vm
   *
   * @param javaBinDir - the path to the bin directory of java
   * @return true if the VM is known not to support the -server arg
   */
  private static boolean omitDashServerArg(final File javaBinDir) {
    final String vendor = System.getProperty("java.vm.vendor");

    if (vendor != null) {
      final String vendorUpperCase = vendor.toUpperCase();

      if (vendorUpperCase.startsWith("IBM")) {
        return true;
      }

      if (vendorUpperCase.startsWith("SUN")) {
        final String os = System.getProperty("os.name");

        if (os != null && os.indexOf("Windows") != -1) {
          final File serverDir = new File(javaBinDir, "server");

          // On Windows with a Sun JVM and there is no ${java.home}/bin/server directory
          // This is true for the 32bit JRE, but not for the JDK
          // Note: this also returns true for 64 bit VMs but that is ok because -server is the
          // default.
          return !serverDir.isDirectory();
        }
      }
    }

    return false;
  }

}
