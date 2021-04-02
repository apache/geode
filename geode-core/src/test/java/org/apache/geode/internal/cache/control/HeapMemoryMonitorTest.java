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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.fake.Fakes;

public class HeapMemoryMonitorTest {

  private HeapMemoryMonitor heapMonitor;
  private Function function;
  private Set memberSet;
  private DistributedMember member;
  private InternalDistributedMember myself;
  private ResourceAdvisor resourceAdvisor;
  private int previousMemoryStateChangeTolerance;
  private static final int memoryStateChangeTolerance = 3;
  private static final String LOW_MEMORY_REGEX =
      "Function: null cannot be executed because the members.*are running low on memory";
  private static final int criticalUsedBytes = 95;
  private static final int evictionUsedBytes = 85;
  private static final int normalUsedBytes = 60;
  private static final int testMemoryEventTolerance = 3;


  @Before
  public void setup() {
    InternalCache internalCache = Fakes.cache();
    DistributedSystem distributedSystem = mock(DistributedSystem.class);
    function = mock(Function.class);
    member = mock(InternalDistributedMember.class);
    myself = mock(InternalDistributedMember.class);
    resourceAdvisor = mock(ResourceAdvisor.class);

    when(internalCache.getDistributedSystem()).thenReturn(distributedSystem);
    when(internalCache.getDistributionAdvisor()).thenReturn(resourceAdvisor);
    when(internalCache.getMyId()).thenReturn(myself);

    heapMonitor = new HeapMemoryMonitor(mock(InternalResourceManager.class), internalCache,
        mock(ResourceManagerStats.class), mock(TenuredHeapConsumptionMonitor.class));
    previousMemoryStateChangeTolerance = heapMonitor.getMemoryStateChangeTolerance();
    heapMonitor.setMemoryStateChangeTolerance(memoryStateChangeTolerance);
    memberSet = new HashSet<>();
    memberSet.add(member);
    heapMonitor.setMostRecentEvent(new MemoryEvent(InternalResourceManager.ResourceType.HEAP_MEMORY,
        MemoryThresholds.MemoryState.DISABLED, MemoryThresholds.MemoryState.DISABLED, null, 0L,
        true, null)); // myself is not critical
  }

  @After
  public void cleanup() {
    heapMonitor.setMemoryStateChangeTolerance(previousMemoryStateChangeTolerance);
  }

  // ========== tests for getHeapCriticalMembersFrom ==========
  @Test
  public void getHeapCriticalMembersFrom_WithEmptyCriticalMembersReturnsEmptySet() {
    getHeapCriticalMembersFrom_returnsEmptySet(Collections.emptySet(), memberSet);
  }

  @Test
  public void getHeapCriticalMembersFrom_WithEmptyArgReturnsEmptySet() {
    getHeapCriticalMembersFrom_returnsEmptySet(memberSet, Collections.emptySet());
  }

  @Test
  public void getHeapCriticalMembersFromWithEmptySetsReturnsEmptySet() {
    getHeapCriticalMembersFrom_returnsEmptySet(Collections.emptySet(), Collections.emptySet());
  }

  @Test
  public void getHeapCriticalMembersFrom_WithDisjointSetsReturnsEmptySet() {
    Set argSet = new HashSet();
    argSet.add(mock(InternalDistributedMember.class));

    getHeapCriticalMembersFrom_returnsEmptySet(memberSet, argSet);
  }

  @Test
  public void getHeapCriticalMembersFrom_WithEqualSetsReturnsMember() {
    getHeapCriticalMembersFrom_returnsNonEmptySet(memberSet, Collections.unmodifiableSet(memberSet),
        new HashSet(memberSet));
  }

  @Test
  public void getHeapCriticalMembersFrom_ReturnsMultipleMembers() {
    DistributedMember member1 = mock(InternalDistributedMember.class);
    DistributedMember member2 = mock(InternalDistributedMember.class);
    DistributedMember member3 = mock(InternalDistributedMember.class);
    DistributedMember member4 = mock(InternalDistributedMember.class);
    Set advisorSet = new HashSet();
    advisorSet.add(member1);
    advisorSet.add(member2);
    advisorSet.add(member4);
    Set argSet = new HashSet(memberSet);
    argSet.add(member1);
    argSet.add(member3);
    argSet.add(member4);
    Set expectedResult = new HashSet();
    expectedResult.add(member1);
    expectedResult.add(member4);

    getHeapCriticalMembersFrom_returnsNonEmptySet(advisorSet, argSet, expectedResult);
  }

  @Test
  public void getHeapCriticalMembersFrom_DoesNotReturnMyselfWhenNotCritical() {
    Set expectedResult = new HashSet(memberSet);
    Set advisorSet = new HashSet(memberSet);
    memberSet.add(myself);

    getHeapCriticalMembersFrom_returnsNonEmptySet(advisorSet,
        Collections.unmodifiableSet(memberSet),
        expectedResult);
  }

  @Test
  public void getHeapCriticalMembersFrom_IncludesMyselfWhenCritical() throws Exception {
    Set advisorSet = new HashSet(memberSet);
    heapMonitor.setMostRecentEvent(new MemoryEvent(InternalResourceManager.ResourceType.HEAP_MEMORY,
        MemoryThresholds.MemoryState.DISABLED, MemoryThresholds.MemoryState.CRITICAL, null, 0L,
        true, null));
    memberSet.add(myself);

    getHeapCriticalMembersFrom_returnsNonEmptySet(advisorSet,
        Collections.unmodifiableSet(memberSet),
        new HashSet(memberSet));
  }

  // ========== tests for createLowMemoryIfNeeded (with Set argument) ==========
  @Test
  public void createLowMemoryIfNeededWithSetArg_ReturnsNullWhenNotOptimizedForWrite()
      throws Exception {
    createLowMemoryIfNeededWithSetArg_returnsNull(false, false, memberSet);
  }

  @Test
  public void createLowMemoryIfNeededWithSetArg_ReturnsNullWhenLowMemoryExceptionDisabled()
      throws Exception {
    createLowMemoryIfNeededWithSetArg_returnsNull(true, true, memberSet);
  }

  @Test
  public void createLowMemoryIfNeededWithSetArg_ReturnsNullWhenNoCriticalMembers()
      throws Exception {
    createLowMemoryIfNeededWithSetArg_returnsNull(true, false, Collections.emptySet());
  }

  @Test
  public void createLowMemoryIfNeededWithSetArg_ReturnsException() throws Exception {
    setMocking(true, false, memberSet);

    LowMemoryException exception = heapMonitor.createLowMemoryIfNeeded(function, memberSet);

    assertLowMemoryException(exception);
  }

  // ========== tests for createLowMemoryIfNeeded (with DistributedMember argument) ==========
  @Test
  public void createLowMemoryIfNeededWithMemberArg_ReturnsNullWhenNotOptimizedForWrite()
      throws Exception {
    createLowMemoryIfNeededWithMemberArg_returnsNull(false, false, member);
  }

  @Test
  public void createLowMemoryIfNeededWithMemberArg_ReturnsNullWhenLowMemoryExceptionDisabled()
      throws Exception {
    createLowMemoryIfNeededWithMemberArg_returnsNull(true, true, member);
  }

  @Test
  public void createLowMemoryIfNeededWithMemberArg_ReturnsNullWhenNoCriticalMembers()
      throws Exception {
    createLowMemoryIfNeededWithMemberArg_returnsNull(true, false, member);
  }

  @Test
  public void createLowMemoryIfNeededWithMemberArg_ReturnsException() throws Exception {
    setMocking(true, false, memberSet);

    LowMemoryException exception = heapMonitor.createLowMemoryIfNeeded(function, member);

    assertLowMemoryException(exception);
  }

  // ========== tests for checkForLowMemory (with Set argument) ==========
  @Test
  public void checkForLowMemoryWithSetArg_DoesNotThrowWhenNotOptimizedForWrite() throws Exception {
    checkForLowMemoryWithSetArg_doesNotThrow(false, false, memberSet);
  }

  @Test
  public void checkForLowMemoryWithSetArg_DoesNotThrowWhenLowMemoryExceptionDisabled()
      throws Exception {
    checkForLowMemoryWithSetArg_doesNotThrow(true, true, memberSet);
  }

  @Test
  public void checkForLowMemoryWithSetArg_DoesNotThrowWhenNoCriticalMembers() throws Exception {
    checkForLowMemoryWithSetArg_doesNotThrow(true, false, Collections.emptySet());
  }

  @Test
  public void checkForLowMemoryWithSetArg_ThrowsLowMemoryException() throws Exception {
    setMocking(true, false, memberSet);

    assertThatThrownBy(() -> heapMonitor.checkForLowMemory(function, memberSet))
        .isExactlyInstanceOf(LowMemoryException.class);
  }

  // ========== tests for checkForLowMemory (with DistributedMember argument) ==========
  @Test
  public void checkForLowMemoryIfNeededWithMemberArg_ReturnsNullWhenNotOptimizedForWrite()
      throws Exception {
    checkForLowMemoryWithMemberArg_doesNotThrow(false, false, member);
  }

  @Test
  public void checkForLowMemoryIfNeededWithMemberArg_ReturnsNullWhenLowMemoryExceptionDisabled()
      throws Exception {
    checkForLowMemoryWithMemberArg_doesNotThrow(true, true, member);
  }

  @Test
  public void checkForLowMemoryIfNeededWithMemberArg_ReturnsNullWhenNoCriticalMembers()
      throws Exception {
    checkForLowMemoryWithMemberArg_doesNotThrow(true, false, member);
  }

  @Test
  public void checkForLowMemoryWithMemberArg_ReturnsException() throws Exception {
    setMocking(true, false, memberSet);

    assertThatThrownBy(() -> heapMonitor.checkForLowMemory(function, member))
        .isExactlyInstanceOf(LowMemoryException.class).hasMessageMatching(LOW_MEMORY_REGEX);
  }

  // ========== tests for updateStateAndSendEvent ==========
  @Test
  public void updateStateAndSendEvent_ThrashingShouldNotChangeState_CriticalAndEvictionEnabled() {
    // Initialize the most recent state to NORMAL
    setupHeapMonitorThresholds(true, true);

    // If we thrash between CRITICAL_EVICTION and NORMAL, we don't expect a state transition
    // to happen because we have a memoryStateChangeTolerance of 3 in this test. We only expect
    // a state transition if threshold value + 1 consecutive events have been received above
    // the critical threshold.
    sendAlternatingEventsAndAssertState(MemoryThresholds.MemoryState.NORMAL);
  }

  @Test
  public void updateStateAndSendEvent_ThrashingShouldNotChangeState_CriticalOnlyEnabled() {
    // Initialize the most recent state to NORMAL
    setupHeapMonitorThresholds(false, true);

    // If we thrash between EVICTION_DISABLED_CRITICAL and NORMAL, we don't expect a state
    // transition
    // to happen because we have a memoryStateChangeTolerance of 3 in this test. We only expect
    // a state transition if threshold value + 1 consecutive events have been received above
    // the critical threshold.
    sendAlternatingEventsAndAssertState(MemoryThresholds.MemoryState.EVICTION_DISABLED);
  }

  @Test
  public void updateStateAndSendEvent_ThrashingShouldNotChangeState_EvictionOnlyEnabled() {
    // Initialize the most recent state to NORMAL
    setupHeapMonitorThresholds(true, false);

    // If we thrash between EVICTION_CRITICAL_DISABLED and NORMAL, we don't expect a state
    // transition
    // to happen because we have a memoryStateChangeTolerance of 3 in this test. We only expect
    // a state transition if threshold value + 1 consecutive events have been received above
    // the critical threshold.
    sendAlternatingEventsAndAssertState(MemoryThresholds.MemoryState.CRITICAL_DISABLED);
  }

  @Test
  public void updateStateAndSendEvent_AboveCriticalMoreThanEventTolerance() {
    setupHeapMonitorThresholds(true, true);

    // It will take 4 consecutive events above the critical threshold to cause a state transition
    // given our memoryStateChangeTolerance of 3 in this test.
    sendEventAndAssertState(criticalUsedBytes, testMemoryEventTolerance,
        MemoryThresholds.MemoryState.NORMAL);
    sendEventAndAssertState(criticalUsedBytes, 1, MemoryThresholds.MemoryState.EVICTION_CRITICAL);
  }

  @Test
  public void updateStateAndSendEvent_AboveCriticalTwoEventsThenAboveEviction() {
    // Initialize the most recent state to NORMAL
    setupHeapMonitorThresholds(true, true);

    // The first three events are above the CRITICAL threshold and will count towards the
    // memoryStateChangeTolerance of 3, but the last event is only above the eviction
    // threshold so we expect the state transition to be from NORMAL to EVICTION.
    sendEventAndAssertState(criticalUsedBytes, testMemoryEventTolerance,
        MemoryThresholds.MemoryState.NORMAL);
    sendEventAndAssertState(evictionUsedBytes, 1, MemoryThresholds.MemoryState.EVICTION);
  }

  @Test
  public void updateStateAndSendEvent_ThreeEvictionsThenCriticalTransitionEvictionCritical() {
    // Initialize the most recent state to NORMAL
    setupHeapMonitorThresholds(true, true);

    // The first three events are above the EVICTION threshold and will count towards the
    // memoryStateChangeTolerance of 3, but the last event is only above the eviction
    // threshold so we expect the state transition to be from NORMAL to EVICTION_CRITICAL.
    sendEventAndAssertState(evictionUsedBytes, testMemoryEventTolerance,
        MemoryThresholds.MemoryState.NORMAL);
    sendEventAndAssertState(criticalUsedBytes, 1, MemoryThresholds.MemoryState.EVICTION_CRITICAL);
  }

  @Test
  public void updateStateAndSendEvent_EvictionDisabledTransitionToCritical() {
    // In this test, the EVICTION threshold is disabled, so we'd expect a transition from
    // EVICTION_DISABLED to EVICTION_DISABLED_CRITICAL after the memoryStateChangeTolerance
    // of 3 is exceeded.
    setupHeapMonitorThresholds(false, true);

    sendEventAndAssertState(criticalUsedBytes, testMemoryEventTolerance,
        MemoryThresholds.MemoryState.EVICTION_DISABLED);
    sendEventAndAssertState(criticalUsedBytes, 1,
        MemoryThresholds.MemoryState.EVICTION_DISABLED_CRITICAL);
  }

  @Test
  public void updateStateAndSendEvent_CriticalDisabledTransitionToEviction() {
    // In this test, the CRITICAL threshold is disabled, so we'd expect a transition from
    // CRITICAL_DISABLED to EVICTION_CRITICAL_DISABLED after the memoryStateChangeTolerance
    // of 3 is exceeded.
    setupHeapMonitorThresholds(true, false);

    // It should take 4 above critical events for the state transition to take effect, because
    // our memory state change tolerance is set to 3 for this test
    sendEventAndAssertState(evictionUsedBytes, testMemoryEventTolerance,
        MemoryThresholds.MemoryState.CRITICAL_DISABLED);
    sendEventAndAssertState(evictionUsedBytes, 1,
        MemoryThresholds.MemoryState.EVICTION_CRITICAL_DISABLED);
  }

  @Test
  public void updateStateAndSendEvent_TogglingBetweenEvictionAndCritical_StatesTransition() {
    setupHeapMonitorThresholds(true, true);

    sendEventAndAssertState(criticalUsedBytes, testMemoryEventTolerance,
        MemoryThresholds.MemoryState.NORMAL);
    // Once in the EVICTION state, the transition between EVICTION and CRITICAL should not
    // depend on the threshold counter
    sendEventAndAssertState(evictionUsedBytes, 1, MemoryThresholds.MemoryState.EVICTION);
    sendEventAndAssertState(criticalUsedBytes, 1, MemoryThresholds.MemoryState.EVICTION_CRITICAL);
    sendEventAndAssertState(evictionUsedBytes, 1, MemoryThresholds.MemoryState.EVICTION);
    sendEventAndAssertState(criticalUsedBytes, 1, MemoryThresholds.MemoryState.EVICTION_CRITICAL);
  }

  @Test
  public void updateStateAndSendEvent_NormalToCriticalToNormalToCritical_ThresholdReset() {
    setupHeapMonitorThresholds(true, true);

    sendEventAndAssertState(criticalUsedBytes, testMemoryEventTolerance,
        MemoryThresholds.MemoryState.NORMAL);
    sendEventAndAssertState(criticalUsedBytes, 1, MemoryThresholds.MemoryState.EVICTION_CRITICAL);
    sendEventAndAssertState(normalUsedBytes, 1, MemoryThresholds.MemoryState.NORMAL);
    // Threshold counter should have been reset, so we need thre more events in the CRITICAL range
    // to trigger a state transition
    sendEventAndAssertState(criticalUsedBytes, testMemoryEventTolerance,
        MemoryThresholds.MemoryState.NORMAL);
    sendEventAndAssertState(criticalUsedBytes, 1, MemoryThresholds.MemoryState.EVICTION_CRITICAL);
  }

  // ========== private methods ==========
  private void setupHeapMonitorThresholds(boolean enableEviction, boolean enableCritical) {
    // Initialize the most recent state to NORMAL
    heapMonitor = spy(heapMonitor);

    // This will prevent the polling monitor from firing and causing state transitions. We
    // want complete control over the state transitions in this test.
    heapMonitor.started = true;

    HeapMemoryMonitor.setTestBytesUsedForThresholdSet(50);
    heapMonitor.setTestMaxMemoryBytes(100);

    if (enableCritical) {
      heapMonitor.setCriticalThreshold(90f);
    }

    if (enableEviction) {
      heapMonitor.setEvictionThreshold(80f);
    }
  }

  private void sendEventAndAssertState(int bytesUsed, int numEvents,
      MemoryThresholds.MemoryState expectedState) {
    for (int i = 0; i < numEvents; ++i) {
      heapMonitor.updateStateAndSendEvent(bytesUsed, "test");
      assertThat(heapMonitor.getState()).isEqualByComparingTo(expectedState);
    }
  }

  private void sendAlternatingEventsAndAssertState(MemoryThresholds.MemoryState expectedState) {
    // testMemoryEventTolerance * 2 is somewhat arbitrary - we just want to test that we don't
    // change states after exceeding the tolerance value if the events alternate between critical
    // and normal used bytes.
    for (int i = 0; i < testMemoryEventTolerance * 2; ++i) {
      // Alternate between normal bytes and critical bytes using modular arithmetic
      if (i % 2 == 0) {
        sendEventAndAssertState(normalUsedBytes, 1, expectedState);
      } else {
        sendEventAndAssertState(criticalUsedBytes, 1, expectedState);
      }
    }
  }

  private void getHeapCriticalMembersFrom_returnsEmptySet(Set adviseCriticalMembers, Set argSet) {
    when(resourceAdvisor.adviseCriticalMembers()).thenReturn(adviseCriticalMembers);

    Set<DistributedMember> criticalMembers = heapMonitor.getHeapCriticalMembersFrom(argSet);

    assertThat(criticalMembers).isEmpty();
  }

  private void getHeapCriticalMembersFrom_returnsNonEmptySet(Set adviseCriticalMembers, Set argSet,
      Set expectedResult) {
    when(resourceAdvisor.adviseCriticalMembers()).thenReturn(adviseCriticalMembers);

    Set<DistributedMember> criticalMembers = heapMonitor.getHeapCriticalMembersFrom(argSet);

    assertThat(criticalMembers).containsAll(expectedResult);
  }

  private void createLowMemoryIfNeededWithSetArg_returnsNull(boolean optimizeForWrite,
      boolean isLowMemoryExceptionDisabled, Set memberSetArg) throws Exception {
    setMocking(optimizeForWrite, isLowMemoryExceptionDisabled, memberSetArg);

    LowMemoryException exception = heapMonitor.createLowMemoryIfNeeded(function, memberSetArg);

    assertThat(exception).isNull();
  }

  private void createLowMemoryIfNeededWithMemberArg_returnsNull(boolean optimizeForWrite,
      boolean isLowMemoryExceptionDisabled, DistributedMember memberArg) throws Exception {
    setMocking(optimizeForWrite, isLowMemoryExceptionDisabled, Collections.emptySet());

    LowMemoryException exception = heapMonitor.createLowMemoryIfNeeded(function, memberArg);

    assertThat(exception).isNull();
  }

  private void checkForLowMemoryWithSetArg_doesNotThrow(boolean optimizeForWrite,
      boolean isLowMemoryExceptionDisabled, Set memberSetArg) throws Exception {
    setMocking(optimizeForWrite, isLowMemoryExceptionDisabled, memberSetArg);

    heapMonitor.checkForLowMemory(function, memberSetArg);
  }

  private void checkForLowMemoryWithMemberArg_doesNotThrow(boolean optimizeForWrite,
      boolean isLowMemoryExceptionDisabled, DistributedMember memberArg) throws Exception {
    setMocking(optimizeForWrite, isLowMemoryExceptionDisabled, Collections.emptySet());

    heapMonitor.checkForLowMemory(function, memberArg);
  }

  private void setMocking(boolean optimizeForWrite, boolean isLowMemoryExceptionDisabled,
      Set argSet) throws Exception {
    when(function.optimizeForWrite()).thenReturn(optimizeForWrite);
    setIsLowMemoryExceptionDisabled(isLowMemoryExceptionDisabled);
    when(resourceAdvisor.adviseCriticalMembers()).thenReturn(argSet);
  }

  private void assertLowMemoryException(LowMemoryException exception) {
    assertThat(exception).isExactlyInstanceOf(LowMemoryException.class);
    assertThat(exception.getMessage()).containsPattern(LOW_MEMORY_REGEX);
  }

  private void setIsLowMemoryExceptionDisabled(boolean isLowMemoryExceptionDisabled)
      throws Exception {
    MemoryThresholds.setLowMemoryExceptionDisabled(isLowMemoryExceptionDisabled);
  }
}
