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
package org.apache.geode.internal.cache.control;


import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionConfig;

public class HeapMemoryMonitorDisableLowMemoryExceptionTest extends HeapMemoryMonitorTestBase {

  private static String savedParam = "false";

  @BeforeClass
  public static void setupSystemProperties() {
    savedParam =
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "disableLowMemoryException", "true");
    assertTrue(savedParam == null || savedParam.equals("false"));
    assertTrue(System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "disableLowMemoryException")
        .equals("true"));
  }

  @AfterClass
  public static void resetSystemProperties() {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "disableLowMemoryException",
        savedParam == null ? "false" : savedParam);
    assertTrue(System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "disableLowMemoryException")
        .equals("false"));
  }

  // ========== tests for createLowMemoryIfNeeded (with Set argument) ==========
  @Test
  public void createLowMemoryIfNeededWithSetArg_ReturnsNullWhenLowMemoryExceptionDisabled()
      throws Exception {
    createLowMemoryIfNeededWithSetArg_returnsNull(true, true, memberSet);
  }

  // ========== tests for checkForLowMemory (with Set argument) ==========
  @Test
  public void checkForLowMemoryWithSetArg_DoesNotThrowWhenLowMemoryExceptionDisabled()
      throws Exception {
    checkForLowMemoryWithSetArg_doesNotThrow(true, true, memberSet);
  }

}
