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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.logging.AbstractExecutor;
import org.apache.geode.internal.logging.ThreadsMonitoring.Mode;

/**
 * Contains simple tests for the {@link org.apache.geode.internal.monitoring.ThreadsMonitoringImpl}.
 *
 * @since Geode 1.5
 */
public class ThreadsMonitoringImplJUnitTest {

  private ThreadsMonitoringImpl threadsMonitoringImpl;

  @Before
  public void before() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null);
  }

  @After
  public void after() {
    threadsMonitoringImpl.close();
  }

  /**
   * Tests "start monitor" modes
   */
  @Test
  public void testStartMonitor() {
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.FunctionExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.PooledExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.SerialQueuedExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.OneTaskOnlyExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.ScheduledThreadExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.AGSExecutor));
  }

  /**
   * Tests closure
   */
  @Test
  public void testClosure() {
    assertTrue(threadsMonitoringImpl.getThreadsMonitoringProcess() != null);
    assertFalse(threadsMonitoringImpl.isClosed());
    threadsMonitoringImpl.close();
    assertTrue(threadsMonitoringImpl.isClosed());
    assertFalse(threadsMonitoringImpl.getThreadsMonitoringProcess() != null);
  }

  @Test
  public void updateThreadStatus() {
    AbstractExecutor executor = mock(AbstractExecutor.class);
    long threadId = Thread.currentThread().getId();

    threadsMonitoringImpl.getMonitorMap().put(threadId, executor);
    threadsMonitoringImpl.updateThreadStatus();

    // also test the case where there is no AbstractExcecutor present
    threadsMonitoringImpl.getMonitorMap().remove(threadId);
    threadsMonitoringImpl.updateThreadStatus();
    verify(executor, times(1)).setStartTime(any(Long.class));
  }
}
