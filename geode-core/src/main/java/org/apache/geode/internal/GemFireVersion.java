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
package org.apache.geode.internal;

import static org.apache.geode.internal.VersionDescription.BUILD_ID;
import static org.apache.geode.internal.VersionDescription.BUILD_JAVA_VERSION;
import static org.apache.geode.internal.VersionDescription.BUILD_PLATFORM;
import static org.apache.geode.internal.VersionDescription.PRODUCT_NAME;
import static org.apache.geode.internal.VersionDescription.PRODUCT_VERSION;
import static org.apache.geode.internal.VersionDescription.RESOURCE_NAME;
import static org.apache.geode.internal.VersionDescription.SOURCE_DATE;
import static org.apache.geode.internal.VersionDescription.SOURCE_REPOSITORY;
import static org.apache.geode.internal.VersionDescription.SOURCE_REVISION;
import static org.apache.geode.internal.lang.SystemUtils.getBootClassPath;
import static org.apache.geode.internal.lang.SystemUtils.getClassPath;

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.Map;
import java.util.StringTokenizer;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.Immutable;

/**
 * This class provides build and version information about GemFire. It gathers this information from
 * the resource property file for this class.
 */
public class GemFireVersion {
  @Immutable
  private static final VersionDescription description = createDescription();

  private GemFireVersion() {}

  private static VersionDescription createDescription() {
    String name =
        GemFireVersion.class.getPackage().getName().replace('.', '/') + "/" + RESOURCE_NAME;
    return new VersionDescription(name);
  }

  private static VersionDescription getDescription() {
    return description;
  }

  public static String getProductName() {
    return getDescription().getProperty(PRODUCT_NAME);
  }

  public static String getGemFireVersion() {
    return getDescription().getProperty(PRODUCT_VERSION);
  }

  public static String getSourceDate() {
    return getDescription().getProperty(SOURCE_DATE);
  }

  public static String getSourceRepository() {
    return getDescription().getProperty(SOURCE_REPOSITORY);
  }

  public static String getSourceRevision() {
    return getDescription().getProperty(SOURCE_REVISION);
  }

  public static String getBuildId() {
    return getDescription().getProperty(BUILD_ID);
  }

  public static String getBuildPlatform() {
    return getDescription().getProperty(BUILD_PLATFORM);
  }

  public static String getBuildJavaVersion() {
    return getDescription().getProperty(BUILD_JAVA_VERSION);
  }

  public static String getGemFireJarFileName() {
    return "geode-core-" + GemFireVersion.getGemFireVersion() + ".jar";
  }

  public static void print(PrintWriter pw) {
    getDescription().print(pw);
  }

  public static void print(PrintStream ps) {
    print(new PrintWriter(ps, true));
  }

  public static String asString() {
    StringWriter sw = new StringWriter(256);
    PrintWriter pw = new PrintWriter(sw);
    print(pw);
    pw.flush();
    return sw.toString();
  }

  /** Public method that returns the URL of the gemfire jar file */
  public static URL getJarURL() {
    java.security.CodeSource cs = GemFireVersion.class.getProtectionDomain().getCodeSource();
    if (cs != null) {
      return cs.getLocation();
    }
    // fix for bug 33274 - null CodeSource from protection domain in Sybase
    URL csLoc = null;
    StringTokenizer tokenizer = new StringTokenizer(getClassPath(), File.pathSeparator);
    while (tokenizer.hasMoreTokens()) {
      String jar = tokenizer.nextToken();
      if (jar.contains(getGemFireJarFileName())) {
        File gemfireJar = new File(jar);
        try {
          csLoc = gemfireJar.toURI().toURL();
        } catch (Exception ignored) {
        }
        break;
      }
    }
    if (csLoc != null) {
      return csLoc;
    }
    // try the boot class path to fix bug 37394
    tokenizer = new StringTokenizer(getBootClassPath(), File.pathSeparator);
    while (tokenizer.hasMoreTokens()) {
      String jar = tokenizer.nextToken();
      if (jar.contains(getGemFireJarFileName())) {
        File gemfireJar = new File(jar);
        try {
          csLoc = gemfireJar.toURI().toURL();
        } catch (Exception ignored) {
        }
        break;
      }
    }
    return csLoc;
  }

  public static @NotNull Map<String, String> asMap() {
    return description.asMap();
  }
}
