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
package org.apache.geode.internal.monitoring.executor;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.lang.management.ThreadInfo;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.internal.monitoring.ThreadsMonitoringProcess;
import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * Contains simple tests for the {@link AbstractExecutor}.
 *
 *
 * @since Geode 1.5
 */
public class AbstractExecutorGroupJUnitTest {

  private final AbstractExecutor abstractExecutorGroup =
      new FunctionExecutionPooledExecutorGroup();

  private static final long timeoutInMilliseconds = GeodeAwaitility.getTimeout().toMillis();

  @Test
  public void testInitializationValues() {
    assertTrue(abstractExecutorGroup.getStartTime() <= System.currentTimeMillis());
    assertTrue(abstractExecutorGroup.getNumIterationsStuck() == 0);
    assertTrue(abstractExecutorGroup.getThreadID() == Thread.currentThread().getId());
  }

  @Test
  public void testWorkFlow() {
    abstractExecutorGroup.handleExpiry(12, new HashMap<>());
    assertTrue(abstractExecutorGroup.getNumIterationsStuck() == 1);
  }

  /**
   * If a thread is blocked by another thread we want to see the other thread's
   * stack in a "stuck thread" report. This test creates such a thread and
   * generates a "stuck thread" report to make sure the report on the other thread
   * is included.
   */
  @Test
  public void lockOwnerThreadStackIsReported() throws InterruptedException {
    final Object syncObject = new Object();
    final Object releaseObject = new Object();
    final boolean[] blockingThreadWaiting = new boolean[1];
    final boolean[] blockedThreadWaiting = new boolean[1];
    Thread blockingThread = new Thread("blocking thread") {
      public void run() {
        synchronized (syncObject) {
          synchronized (releaseObject) {
            try {
              blockingThreadWaiting[0] = true;
              releaseObject.wait(timeoutInMilliseconds);
            } catch (InterruptedException e) {
              return;
            }
          }
        }
      }
    };
    Thread blockedThread = new Thread("blocked thread") {
      public void run() {
        blockedThreadWaiting[0] = true;
        synchronized (syncObject) {
        }
      }
    };
    blockingThread.start();
    await().until(() -> blockingThreadWaiting[0]);
    blockedThread.start();
    await().until(() -> blockedThreadWaiting[0]);
    try {
      AbstractExecutor executor = new AbstractExecutor("testGroup", blockedThread.getId()) {
        @Override
        public void handleExpiry(long stuckTime, Map<Long, ThreadInfo> map) {
          // no-op
        }
      };
      await().untilAsserted(() -> {
        Set<Long> threadIds = new HashSet<>();
        threadIds.add(blockedThread.getId());
        threadIds.add(blockingThread.getId());
        String threadReport = executor.createThreadReport(60000,
            ThreadsMonitoringProcess.createThreadInfoMap(threadIds, true, true));
        assertThat(threadReport)
            .contains(AbstractExecutor.LOCK_OWNER_THREAD_STACK + " for \"blocking thread\"");
        assertThat(threadReport).contains("Waiting on <" + syncObject + ">");
        assertThat(threadReport).contains("Owned By <blocking thread>");
        assertThat(threadReport).contains("- locked " + syncObject);
      });
    } finally {
      blockingThread.interrupt();
      blockedThread.interrupt();
      blockingThread.join(timeoutInMilliseconds);
      blockedThread.join(timeoutInMilliseconds);
    }
  }
}
