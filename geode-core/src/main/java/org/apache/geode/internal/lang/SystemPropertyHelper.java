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

/**
 * The SystemPropertyHelper class is an helper class for accessing system properties used in geode.
 * The method name to get the system property should be the same as the system property name.
 *
 * @since Geode 1.4.0
 */

public class SystemPropertyHelper {
  private static final String GEODE_PREFIX = "geode.";
  private static final String GEMFIRE_PREFIX = "gemfire.";

  /**
   * This method will try to look up "geode." and "gemfire." versions of the system property. It
   * will check and prefer "geode." setting first, then try to check "gemfire." setting.
   *
   * @param name system property name set in Geode
   * @return a boolean value of the system property
   */
  private static boolean getProductBooleanProperty(String name) {
    String property = System.getProperty(GEODE_PREFIX + name);
    if (property != null) {
      return Boolean.getBoolean(GEODE_PREFIX + name);
    }
    return Boolean.getBoolean(GEMFIRE_PREFIX + name);
  }

  /**
   * As of Geode 1.4.0, a region set operation will be in a transaction even if it is the first
   * operation in the transaction.
   *
   * In previous releases, a region set operation is not in a transaction if it is the first
   * operation of the transaction.
   *
   * Setting this system property to true will restore the previous behavior.
   *
   * @since Geode 1.4.0
   */
  public static boolean restoreSetOperationTransactionBehavior() {
    return getProductBooleanProperty("restoreSetOperationTransactionBehavior");
  }

  /**
   * As of Geode 1.4.0, idle expiration on a replicate or partitioned region will now do a
   * distributed check for a more recent last access time on one of the other copies of the region.
   *
   * This system property can be set to true to turn off this new check and restore the previous
   * behavior of only using the local last access time as the basis for expiration.
   *
   * @since Geode 1.4.0
   */
  public static boolean restoreIdleExpirationBehavior() {
    return getProductBooleanProperty("restoreIdleExpirationBehavior");
  }
}
