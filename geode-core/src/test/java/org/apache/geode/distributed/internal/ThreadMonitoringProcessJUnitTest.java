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
package org.apache.geode.distributed.internal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.ThreadMonitoring.Mode;
import org.apache.geode.internal.statistics.AbstractExecutorGroup;
import org.apache.geode.internal.statistics.PooledExecutorGroup;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Contains simple tests for the {@link ThreadMonitoringImpl}.
 *
 *
 * @since Geode 1.5
 */
@Category({UnitTest.class})
public class ThreadMonitoringProcessJUnitTest {

  private ThreadMonitoringImpl threadMonitoringImpl;

  @Before
  public void before() {
    threadMonitoringImpl = new ThreadMonitoringImpl();
  }

  /**
   * Tests that indeed thread is considered stuck when it should
   */
  @Test
  public void testThreadIsStuck() {

    final Properties nonDefault = new Properties();
    final DistributionConfigImpl distributionConfigImpl = new DistributionConfigImpl(nonDefault);
    final long threadID = 123456;

    int timeLimit = distributionConfigImpl.getThreadMonitorTimeLimit();

    AbstractExecutorGroup absExtgroup = new PooledExecutorGroup(threadMonitoringImpl);
    absExtgroup.setStartTime(absExtgroup.getStartTime() - timeLimit - 1);

    threadMonitoringImpl.getMonitorMap().put(threadID, absExtgroup);

    assertTrue(threadMonitoringImpl.getThreadMonitoringProcess().mapValidation());

    threadMonitoringImpl.close();
  }

  /**
   * Tests that indeed thread is NOT considered stuck when it shouldn't
   */
  @Test
  public void testThreadIsNotStuck() {

    threadMonitoringImpl.startMonitor(Mode.PooledExecutor);

    assertFalse(threadMonitoringImpl.getThreadMonitoringProcess().mapValidation());

    threadMonitoringImpl.close();
  }
}
