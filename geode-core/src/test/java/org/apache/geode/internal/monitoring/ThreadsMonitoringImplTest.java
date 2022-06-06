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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.geode.internal.monitoring.ThreadsMonitoring.Mode;
import org.apache.geode.internal.monitoring.executor.AbstractExecutor;
import org.apache.geode.internal.monitoring.executor.FunctionExecutionPooledExecutorGroup;
import org.apache.geode.internal.monitoring.executor.PooledExecutorGroup;

/**
 * Contains simple tests for the {@link org.apache.geode.internal.monitoring.ThreadsMonitoringImpl}.
 *
 * @since Geode 1.5
 */
public class ThreadsMonitoringImplTest {
  private static final int TIME_LIMIT_MILLIS = 1000;

  private ThreadsMonitoringImpl threadsMonitoringImpl;

  @AfterEach
  public void after() {
    threadsMonitoringImpl.close();
  }

  /**
   * Tests "start monitor" modes
   */
  @Test
  public void testStartMonitor() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, 0, false);
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.FunctionExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.PooledExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.SerialQueuedExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.OneTaskOnlyExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.ScheduledThreadExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.AGSExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.P2PReaderExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.ServerConnectionExecutor));
  }

  @Test
  public void verifyMonitorLifeCycle() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, 0, false);
    assertFalse(threadsMonitoringImpl.isMonitoring());
    threadsMonitoringImpl.startMonitor(Mode.FunctionExecutor);
    assertTrue(threadsMonitoringImpl.isMonitoring());
    threadsMonitoringImpl.endMonitor();
    assertFalse(threadsMonitoringImpl.isMonitoring());
  }

  @Test
  public void verifyExecutorMonitoringLifeCycle() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, 0, false);
    AbstractExecutor executor =
        threadsMonitoringImpl.createAbstractExecutor(Mode.P2PReaderExecutor);
    assertThat(threadsMonitoringImpl.isMonitoring(executor)).isFalse();
    threadsMonitoringImpl.register(executor);
    assertThat(threadsMonitoringImpl.isMonitoring(executor)).isTrue();
    executor.suspendMonitoring();
    assertThat(threadsMonitoringImpl.isMonitoring(executor)).isFalse();
    executor.resumeMonitoring();
    assertThat(threadsMonitoringImpl.isMonitoring(executor)).isTrue();
    threadsMonitoringImpl.unregister(executor);
    assertThat(threadsMonitoringImpl.isMonitoring(executor)).isFalse();
    threadsMonitoringImpl.register(executor);
    assertThat(threadsMonitoringImpl.isMonitoring(executor)).isTrue();
    threadsMonitoringImpl.unregister(executor);
    assertThat(threadsMonitoringImpl.isMonitoring(executor)).isFalse();
  }

  @Test
  public void createAbstractExecutorIsAssociatedWithCallingThread() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, 0, false);
    AbstractExecutor executor = threadsMonitoringImpl.createAbstractExecutor(Mode.FunctionExecutor);
    assertThat(executor.getThreadID()).isEqualTo(Thread.currentThread().getId());
  }

  @Test
  public void createAbstractExecutorDoesNotSetStartTime() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, 0, false);
    AbstractExecutor executor = threadsMonitoringImpl.createAbstractExecutor(Mode.FunctionExecutor);
    assertThat(executor.getStartTime()).isEqualTo(0);
  }

  @Test
  public void createAbstractExecutorSetsNumIterationsStuckToZero() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, 0, false);
    AbstractExecutor executor = threadsMonitoringImpl.createAbstractExecutor(Mode.FunctionExecutor);
    assertThat(executor.getNumIterationsStuck()).isEqualTo((short) 0);
  }

  @Test
  public void createAbstractExecutorSetsExpectedGroupName() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, 0, false);
    AbstractExecutor executor = threadsMonitoringImpl.createAbstractExecutor(Mode.FunctionExecutor);
    assertThat(executor.getGroupName()).isEqualTo(FunctionExecutionPooledExecutorGroup.GROUPNAME);
  }

  /**
   * Tests closure
   */
  @Test
  public void testClosure() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, 0, true);
    assertNotNull(threadsMonitoringImpl.getThreadsMonitoringProcess());
    assertFalse(threadsMonitoringImpl.isClosed());
    threadsMonitoringImpl.close();
    assertTrue(threadsMonitoringImpl.isClosed());
    assertNull(threadsMonitoringImpl.getThreadsMonitoringProcess());
  }

  @Test
  public void updateThreadStatusCallsSetStartTime() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, 0, false);
    AbstractExecutor executor = mock(AbstractExecutor.class);
    long threadId = Thread.currentThread().getId();

    threadsMonitoringImpl.getMonitorMap().put(threadId, executor);
    threadsMonitoringImpl.updateThreadStatus();
    verify(executor, times(1)).setStartTime(any(Long.class));
  }

  @Test
  public void updateThreadStatusWithoutExecutorInMapDoesNotCallSetStartTime() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, 0, false);
    AbstractExecutor executor = mock(AbstractExecutor.class);
    threadsMonitoringImpl.updateThreadStatus();
    verify(executor, never()).setStartTime(any(Long.class));
  }

  /**
   * Tests that indeed thread is considered stuck when it should
   */
  @Test
  public void testThreadIsStuck() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, TIME_LIMIT_MILLIS);

    final long threadID = 123456;

    AbstractExecutor absExtgroup = new PooledExecutorGroup();
    absExtgroup.setStartTime(absExtgroup.getStartTime() - TIME_LIMIT_MILLIS - 1);

    threadsMonitoringImpl.getMonitorMap().put(threadID, absExtgroup);

    assertTrue(threadsMonitoringImpl.getThreadsMonitoringProcess().mapValidation());
  }

  @Test
  public void monitorHandlesDefunctThread() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, TIME_LIMIT_MILLIS);
    final long threadID = Long.MAX_VALUE;

    AbstractExecutor absExtgroup = new PooledExecutorGroup(threadID);
    absExtgroup.setStartTime(absExtgroup.getStartTime() - TIME_LIMIT_MILLIS - 1);

    threadsMonitoringImpl.getMonitorMap().put(threadID, absExtgroup);

    assertTrue(threadsMonitoringImpl.getThreadsMonitoringProcess().mapValidation());
  }

  /**
   * Tests that indeed thread is NOT considered stuck when it shouldn't
   */
  @Test
  public void testThreadIsNotStuck() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, TIME_LIMIT_MILLIS);

    threadsMonitoringImpl.startMonitor(Mode.PooledExecutor);

    assertFalse(threadsMonitoringImpl.getThreadsMonitoringProcess().mapValidation());
  }

}
