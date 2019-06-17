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

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.statistics.OsStatisticsProvider;

/**
 * Used to determine if product should use pure java mode.
 */
public class PureJavaMode {
  /**
   * System property to set to true to force pure java mode
   */
  public static final String PURE_MODE_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "pureJavaMode";
  /**
   * System property to set to true enable debug information regarding native library loading.
   *
   * @since GemFire 5.1
   */
  public static final String LOADLIBRARY_DEBUG_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "loadLibrary.debug";

  private static final boolean debug = Boolean.getBoolean(LOADLIBRARY_DEBUG_PROPERTY);

  private static final boolean isPure;
  private static final boolean is64Bit;
  static {
    boolean tmpIsPure = false;
    if (Boolean.getBoolean(PURE_MODE_PROPERTY)) {
      if (debug) {
        System.out.println("property " + PURE_MODE_PROPERTY + " is true");
      }
      tmpIsPure = true;
    } else {
      tmpIsPure = false;
      try {
        // Attempting to load the library
        SharedLibrary.loadLibrary(debug);
      } catch (UnsatisfiedLinkError ignore) {
        if (debug) {
          System.out
              .println("java.library.path is set to:\n" + System.getProperty("java.library.path"));
          System.out.println("Error: Failed to load library " + SharedLibrary.getName());
          ignore.printStackTrace();
        }
        tmpIsPure = true;
      }
    }
    isPure = tmpIsPure;
    is64Bit = SharedLibrary.is64Bit();
  }

  public static boolean isPure() {
    return isPure;
  }

  public static boolean is64Bit() {
    return is64Bit;
  }

  @Deprecated
  public static boolean osStatsAreAvailable() {
    return OsStatisticsProvider.build().osStatsSupported();
  }
}
