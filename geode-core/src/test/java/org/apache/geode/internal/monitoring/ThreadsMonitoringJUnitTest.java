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
package org.apache.geode.internal.monitoring;

import static org.junit.Assert.assertTrue;

import org.apache.logging.log4j.Logger;
import org.junit.Test;

import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Contains simple tests for the {@link org.apache.geode.internal.monitoring.ThreadsMonitoring}.
 *
 * @since Geode 1.5
 */
public class ThreadsMonitoringJUnitTest {

  public enum ModeExpected {
    FunctionExecutor,
    PooledExecutor,
    SerialQueuedExecutor,
    OneTaskOnlyExecutor,
    ScheduledThreadExecutor,
    AGSExecutor,
    P2PReaderExecutor
  };


  public final int numberOfElements = 7;
  private static final Logger logger = LogService.getLogger();

  /**
   * Tests that number of elements in ThreadMonitoring.Mode is correct
   */
  @Test
  public void testVerifyNumberOfElements() {
    assertTrue(ThreadsMonitoring.Mode.values().length == numberOfElements);
  }

  /**
   * Tests that type of elements in ThreadMonitoring.Mode is correct
   */
  @Test
  public void testVerifyTypeOfElements() {
    try {
      for (int i = 0; i < ThreadsMonitoring.Mode.values().length; i++) {
        assertTrue(
            ThreadsMonitoring.Mode.values()[i].name().equals(ModeExpected.values()[i].name()));
      }
    } catch (ArrayIndexOutOfBoundsException arrayIndexOutOfBoundsException) {
      logger.error(
          "Please verify to update the test in case of changes in ThreadMonitoring.Mode enum\n",
          arrayIndexOutOfBoundsException);
      assertTrue(false);
    }
  }
}
