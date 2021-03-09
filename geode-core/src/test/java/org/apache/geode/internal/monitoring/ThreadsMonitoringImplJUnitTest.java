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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.monitoring.ThreadsMonitoring.Mode;
import org.apache.geode.internal.monitoring.executor.AbstractExecutor;
import org.apache.geode.internal.monitoring.executor.FunctionExecutionPooledExecutorGroup;

/**
 * Contains simple tests for the {@link org.apache.geode.internal.monitoring.ThreadsMonitoringImpl}.
 *
 * @since Geode 1.5
 */
public class ThreadsMonitoringImplJUnitTest {

  private ThreadsMonitoringImpl threadsMonitoringImpl;

  @Before
  public void before() {
    threadsMonitoringImpl = new ThreadsMonitoringImpl(null, 100000, 0, false);
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
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.P2PReaderExecutor));
    assertTrue(threadsMonitoringImpl.startMonitor(Mode.ServerConnectionExecutor));
  }

  @Test
  public void verifyMonitorLifeCycle() {
    assertFalse(threadsMonitoringImpl.isMonitoring());
    threadsMonitoringImpl.startMonitor(Mode.FunctionExecutor);
    assertTrue(threadsMonitoringImpl.isMonitoring());
    threadsMonitoringImpl.endMonitor();
    assertFalse(threadsMonitoringImpl.isMonitoring());
  }

  @Test
  public void verifyExecutorMonitoringLifeCycle() {
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
    AbstractExecutor executor = threadsMonitoringImpl.createAbstractExecutor(Mode.FunctionExecutor);
    assertThat(executor.getThreadID()).isEqualTo(Thread.currentThread().getId());
  }

  @Test
  public void createAbstractExecutorDoesNotSetStartTime() {
    AbstractExecutor executor = threadsMonitoringImpl.createAbstractExecutor(Mode.FunctionExecutor);
    assertThat(executor.getStartTime()).isEqualTo(0);
  }

  @Test
  public void createAbstractExecutorSetsNumIterationsStuckToZero() {
    AbstractExecutor executor = threadsMonitoringImpl.createAbstractExecutor(Mode.FunctionExecutor);
    assertThat(executor.getNumIterationsStuck()).isEqualTo((short) 0);
  }

  @Test
  public void createAbstractExecutorSetsExpectedGroupName() {
    AbstractExecutor executor = threadsMonitoringImpl.createAbstractExecutor(Mode.FunctionExecutor);
    assertThat(executor.getGroupName()).isEqualTo(FunctionExecutionPooledExecutorGroup.GROUPNAME);
  }

  /**
   * Tests closure
   */
  @Test
  public void testClosure() {
    ThreadsMonitoringImpl liveMonitor = new ThreadsMonitoringImpl(null, 100000, 0, true);
    assertTrue(liveMonitor.getThreadsMonitoringProcess() != null);
    assertFalse(liveMonitor.isClosed());
    liveMonitor.close();
    assertTrue(liveMonitor.isClosed());
    assertFalse(liveMonitor.getThreadsMonitoringProcess() != null);
  }

  @Test
  public void updateThreadStatusCallsSetStartTime() {
    AbstractExecutor executor = mock(AbstractExecutor.class);
    long threadId = Thread.currentThread().getId();

    threadsMonitoringImpl.getMonitorMap().put(threadId, executor);
    threadsMonitoringImpl.updateThreadStatus();
    verify(executor, times(1)).setStartTime(any(Long.class));
  }

  @Test
  public void updateThreadStatusWithoutExecutorInMapDoesNotCallSetStartTime() {
    AbstractExecutor executor = mock(AbstractExecutor.class);
    threadsMonitoringImpl.updateThreadStatus();
    verify(executor, never()).setStartTime(any(Long.class));
  }
}
