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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;

import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;

public class HeapMemoryMonitorTestBase {

  protected HeapMemoryMonitor heapMonitor;
  protected Function function;
  protected Set memberSet;
  protected DistributedMember member;
  protected InternalDistributedMember myself;
  protected ResourceAdvisor resourceAdvisor;
  protected static final String LOW_MEMORY_REGEX =
      "Function: null cannot be executed because the members.*are running low on memory";

  @Before
  public void setup() {
    InternalCache internalCache = mock(InternalCache.class);
    DistributedSystem distributedSystem = mock(DistributedSystem.class);
    function = mock(Function.class);
    member = mock(InternalDistributedMember.class);
    myself = mock(InternalDistributedMember.class);
    resourceAdvisor = mock(ResourceAdvisor.class);

    when(internalCache.getDistributedSystem()).thenReturn(distributedSystem);
    when(internalCache.getDistributionAdvisor()).thenReturn(resourceAdvisor);
    when(internalCache.getMyId()).thenReturn(myself);

    heapMonitor = new HeapMemoryMonitor(null, internalCache, null);
    memberSet = new HashSet<>();
    memberSet.add(member);
    heapMonitor.setMostRecentEvent(new MemoryEvent(InternalResourceManager.ResourceType.HEAP_MEMORY,
        MemoryThresholds.MemoryState.DISABLED, MemoryThresholds.MemoryState.DISABLED, null, 0L,
        true, null)); // myself is not critical
  }

  // ========== protected methods ==========
  protected void getHeapCriticalMembersFrom_returnsEmptySet(Set adviseCriticalMembers, Set argSet) {
    when(resourceAdvisor.adviseCriticalMembers()).thenReturn(adviseCriticalMembers);

    Set<DistributedMember> criticalMembers = heapMonitor.getHeapCriticalMembersFrom(argSet);

    assertThat(criticalMembers).isEmpty();
  }

  protected void getHeapCriticalMembersFrom_returnsNonEmptySet(Set adviseCriticalMembers,
      Set argSet,
      Set expectedResult) {
    when(resourceAdvisor.adviseCriticalMembers()).thenReturn(adviseCriticalMembers);

    Set<DistributedMember> criticalMembers = heapMonitor.getHeapCriticalMembersFrom(argSet);

    assertThat(criticalMembers).containsAll(expectedResult);
  }

  protected void createLowMemoryIfNeededWithSetArg_returnsNull(boolean optimizeForWrite,
      boolean isLowMemoryExceptionDisabled, Set memberSetArg) throws Exception {
    setMocking(optimizeForWrite, isLowMemoryExceptionDisabled, memberSetArg);

    LowMemoryException exception = heapMonitor.createLowMemoryIfNeeded(function, memberSetArg);

    assertThat(exception).isNull();
  }

  protected void createLowMemoryIfNeededWithMemberArg_returnsNull(boolean optimizeForWrite,
      boolean isLowMemoryExceptionDisabled, DistributedMember memberArg) throws Exception {
    setMocking(optimizeForWrite, isLowMemoryExceptionDisabled, Collections.emptySet());

    LowMemoryException exception = heapMonitor.createLowMemoryIfNeeded(function, memberArg);

    assertThat(exception).isNull();
  }

  protected void checkForLowMemoryWithSetArg_doesNotThrow(boolean optimizeForWrite,
      boolean isLowMemoryExceptionDisabled, Set memberSetArg) throws Exception {
    setMocking(optimizeForWrite, isLowMemoryExceptionDisabled, memberSetArg);

    heapMonitor.checkForLowMemory(function, memberSetArg);
  }

  protected void checkForLowMemoryWithMemberArg_doesNotThrow(boolean optimizeForWrite,
      boolean isLowMemoryExceptionDisabled, DistributedMember memberArg) throws Exception {
    setMocking(optimizeForWrite, isLowMemoryExceptionDisabled, Collections.emptySet());

    heapMonitor.checkForLowMemory(function, memberArg);
  }

  protected void setMocking(boolean optimizeForWrite, boolean isLowMemoryExceptionDisabled,
      Set argSet) throws Exception {
    when(function.optimizeForWrite()).thenReturn(optimizeForWrite);
    when(resourceAdvisor.adviseCriticalMembers()).thenReturn(argSet);
  }

  protected void assertLowMemoryException(LowMemoryException exception) {
    assertThat(exception).isExactlyInstanceOf(LowMemoryException.class);
    assertThat(exception.getMessage()).containsPattern(LOW_MEMORY_REGEX);
  }

}
