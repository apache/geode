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
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class ThreadsMonitoringProcessTest {
  ThreadMXBean threadMXBean = mock(ThreadMXBean.class);

  @Test
  public void createThreadInfoMapWithNoIdsReturnsEmptyMap() {
    Map<Long, ThreadInfo> map =
        ThreadsMonitoringProcess.createThreadInfoMap(threadMXBean, Collections.emptySet(), false,
            false);
    assertThat(map).isEmpty();
  }

  @Test
  public void createThreadInfoMapWithIdsNoLocksNoBatchReturnsExpectedResult() {
    Set<Long> pids = createPidSet();
    ThreadInfo t1Info = createThreadInfoForNoLocksNoBatch(1L);
    ThreadInfo t2Info = createThreadInfoForNoLocksNoBatch(2L);
    ThreadInfo t3Info = createThreadInfoForNoLocksNoBatch(3L);
    Map<Long, ThreadInfo> expectedResult = createExpectedResult(t1Info, t2Info, t3Info);

    Map<Long, ThreadInfo> map =
        ThreadsMonitoringProcess.createThreadInfoMap(threadMXBean, pids, false,
            false);

    assertThat(map).isEqualTo(expectedResult);
  }

  @NotNull
  private Map<Long, ThreadInfo> createExpectedResult(ThreadInfo t1Info, ThreadInfo t2Info,
      ThreadInfo t3Info) {
    Map<Long, ThreadInfo> expectedResult = new HashMap<>();
    expectedResult.put(1L, t1Info);
    expectedResult.put(2L, t2Info);
    expectedResult.put(3L, t3Info);
    return expectedResult;
  }

  @NotNull
  private Set<Long> createPidSet() {
    Set<Long> pids = new HashSet<>();
    pids.add(1L);
    pids.add(2L);
    pids.add(3L);
    return pids;
  }

  @NotNull
  private ThreadInfo createThreadInfoForNoLocksNoBatch(long pid) {
    ThreadInfo result = mock(ThreadInfo.class);
    when(result.getThreadId()).thenReturn(pid);
    when(threadMXBean.getThreadInfo(eq(pid), eq(Integer.MAX_VALUE))).thenReturn(result);
    return result;
  }

  @Test
  public void createThreadInfoMapWithIdsLocksNoBatchReturnsExpectedResult() {
    Set<Long> pids = createPidSet();
    ThreadInfo t1Info = createThreadInfoForLocksNoBatch(1L);
    ThreadInfo t2Info = createThreadInfoForLocksNoBatch(2L);
    ThreadInfo t3Info = createThreadInfoForLocksNoBatch(3L);
    Map<Long, ThreadInfo> expectedResult = createExpectedResult(t1Info, t2Info, t3Info);

    Map<Long, ThreadInfo> map =
        ThreadsMonitoringProcess.createThreadInfoMap(threadMXBean, pids, true,
            false);

    assertThat(map).isEqualTo(expectedResult);
  }

  @NotNull
  private ThreadInfo createThreadInfoForLocksNoBatch(long pid) {
    ThreadInfo result = mock(ThreadInfo.class);
    when(result.getThreadId()).thenReturn(pid);
    when(threadMXBean.getThreadInfo(aryEq(new long[] {pid}), eq(true), eq(true))).thenReturn(
        new ThreadInfo[] {result});
    return result;
  }

  @Test
  public void createThreadInfoMapWithIdsLocksBatchReturnsExpectedResult() {
    Set<Long> pids = createPidSet();
    Map<Long, ThreadInfo> expectedResult = createThreadInfoMapForBatch(pids, true);

    Map<Long, ThreadInfo> map =
        ThreadsMonitoringProcess.createThreadInfoMap(threadMXBean, pids, true,
            true);

    assertThat(map).isEqualTo(expectedResult);
  }

  @Test
  public void createThreadInfoMapWithIdsNoLocksBatchReturnsExpectedResult() {
    Set<Long> pids = createPidSet();
    Map<Long, ThreadInfo> expectedResult = createThreadInfoMapForBatch(pids, false);

    Map<Long, ThreadInfo> map =
        ThreadsMonitoringProcess.createThreadInfoMap(threadMXBean, pids, false,
            true);

    assertThat(map).isEqualTo(expectedResult);
  }

  @NotNull
  private Map<Long, ThreadInfo> createThreadInfoMapForBatch(Set<Long> pids, boolean locks) {
    long[] pidArray = new long[pids.size()];
    ThreadInfo[] threadInfoArray = new ThreadInfo[pids.size()];
    Map<Long, ThreadInfo> result = new HashMap<>();
    int idx = 0;
    for (Long pid : pids) {
      pidArray[idx] = pid;
      ThreadInfo threadInfo = mock(ThreadInfo.class);
      when(threadInfo.getThreadId()).thenReturn(pid);
      threadInfoArray[idx] = threadInfo;
      result.put(pid, threadInfo);
      idx++;
    }
    when(threadMXBean.getThreadInfo(aryEq(pidArray), eq(locks), eq(locks)))
        .thenReturn(threadInfoArray);
    return result;
  }
}
