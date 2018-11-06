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

import static org.apache.geode.internal.lang.SystemUtils.getOsArchitecture;
import static org.apache.geode.internal.lang.SystemUtils.getOsName;
import static org.apache.geode.internal.lang.SystemUtils.getOsVersion;

import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.geode.SystemFailure;
import org.apache.geode.internal.net.SocketCreator;

public class VersionDescription {
  public static final String RESOURCE_NAME = "GemFireVersion.properties";

  /**
   * Constant for the GemFire version Resource Property entry
   */
  public static final String PRODUCT_NAME = "Product-Name";

  /**
   * Constant for the GemFire version Resource Property entry
   */
  public static final String PRODUCT_VERSION = "Product-Version";

  /**
   * Constant for the source code date Resource Property entry
   */
  public static final String SOURCE_DATE = "Source-Date";

  /**
   * Constant for the source code revision Resource Property entry
   */
  public static final String SOURCE_REVISION = "Source-Revision";

  /**
   * Constant for the source code repository Resource Property entry
   */
  public static final String SOURCE_REPOSITORY = "Source-Repository";

  /**
   * Constant for the build date Resource Property entry
   */
  public static final String BUILD_DATE = "Build-Date";

  /**
   * Constant for the build id Resource Property entry
   */
  public static final String BUILD_ID = "Build-Id";

  /**
   * Constant for the build Java version Resource Property entry
   */
  public static final String BUILD_PLATFORM = "Build-Platform";

  /**
   * Constant for the build Java version Resource Property entry
   */
  public static final String BUILD_JAVA_VERSION = "Build-Java-Version";

  /**
   * the version properties
   */
  private final Properties description;

  /**
   * Error message to display instead of the version information
   */
  private final Optional<String> error;

  public VersionDescription(String name) {
    InputStream is = ClassPathLoader.getLatest().getResourceAsStream(getClass(), name);
    if (is == null) {
      error = Optional
          .of(String.format("<Could not find resource org/apache/geode/internal/%s>",
              name));
      description = null;
      return;
    }

    description = new Properties();
    try {
      description.load(is);
    } catch (Exception ex) {
      error = Optional
          .of(String.format(
              "<Could not read properties from resource org/apache/geode/internal/%s because: %s>",
              name, ex));
      return;
    }

    error = validate(description);
  }

  public String getProperty(String key) {
    return error.orElseGet(() -> description.getProperty(key));
  }

  private String getNativeCodeVersion() {
    return SmHelper.getNativeVersion();
  }

  void print(PrintWriter pw) {
    if (error.isPresent()) {
      pw.println(error.get());
    } else {
      for (Entry<?, ?> props : new TreeMap<>(description).entrySet()) {
        pw.println(props.getKey() + ": " + props.getValue());
      }
    }

    // not stored in the description map
    pw.println("Native version: " + getNativeCodeVersion());
    printHostInfo(pw);
  }

  private void printHostInfo(PrintWriter pw) throws Error {
    try {
      String sb = SocketCreator.getLocalHost().toString() + ", "
          + Runtime.getRuntime().availableProcessors() + " cpu(s), " + getOsArchitecture() + ' '
          + getOsName() + ' ' + getOsVersion() + ' ';
      pw.println(String.format("Running on: %s", sb));
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
    }
  }

  private Optional<String> validate(Properties props) {
    if (props.get(PRODUCT_NAME) == null) {
      return Optional
          .of(String.format("<Missing property %s from resource org/apache/geode/internal/%s>",
              PRODUCT_NAME, RESOURCE_NAME));
    }

    if (props.get(PRODUCT_VERSION) == null) {
      return Optional
          .of(String.format("<Missing property %s from resource org/apache/geode/internal/%s>",
              PRODUCT_VERSION, RESOURCE_NAME));
    }

    if (props.get(SOURCE_DATE) == null) {
      return Optional
          .of(String.format("<Missing property %s from resource org/apache/geode/internal/%s>",
              SOURCE_DATE, RESOURCE_NAME));
    }

    if (props.get(SOURCE_REVISION) == null) {
      return Optional
          .of(String.format("<Missing property %s from resource org/apache/geode/internal/%s>",
              SOURCE_REVISION, RESOURCE_NAME));
    }

    if (props.get(SOURCE_REPOSITORY) == null) {
      return Optional
          .of(String.format("<Missing property %s from resource org/apache/geode/internal/%s>",
              SOURCE_REPOSITORY, RESOURCE_NAME));
    }

    if (props.get(BUILD_DATE) == null) {
      return Optional
          .of(String.format("<Missing property %s from resource org/apache/geode/internal/%s>",
              BUILD_DATE, RESOURCE_NAME));
    }

    if (props.get(BUILD_ID) == null) {
      return Optional
          .of(String.format("<Missing property %s from resource org/apache/geode/internal/%s>",
              BUILD_ID, RESOURCE_NAME));
    }

    if (props.get(BUILD_PLATFORM) == null) {
      return Optional
          .of(String.format("<Missing property %s from resource org/apache/geode/internal/%s>",
              BUILD_PLATFORM, RESOURCE_NAME));
    }

    if (props.get(BUILD_JAVA_VERSION) == null) {
      return Optional
          .of(String.format("<Missing property %s from resource org/apache/geode/internal/%s>",
              BUILD_JAVA_VERSION, RESOURCE_NAME));
    }
    return Optional.empty();
  }
}
