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
package org.apache.geode.management.internal.cli.functions;

import static java.net.InetAddress.getLocalHost;
import static java.util.Arrays.asList;
import static java.util.Collections.addAll;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.internal.cache.partitioned.ColocatedRegionDetails;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;

public class ShowMissingDiskStoresFunctionTest {

  private InternalCache cache;
  private FunctionContext<Void> functionContext;
  private PersistentMemberManager persistentMemberManager;
  private PartitionedRegion region1;
  private PartitionedRegion region2;
  private CollectingResultSender<Object> resultSender;

  private ShowMissingDiskStoresFunction showMissingDiskStoresFunction;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    cache = mock(InternalCache.class);
    persistentMemberManager = mock(PersistentMemberManager.class);
    region1 = mock(PartitionedRegion.class);
    region2 = mock(PartitionedRegion.class);
    resultSender = new CollectingResultSender<>();

    functionContext = new FunctionContextImpl(cache, "testFunction", null, resultSender);
    showMissingDiskStoresFunction = new ShowMissingDiskStoresFunction();
  }

  @Test
  public void execute_resultsContains_null_whenNoMissingDiskStores() {
    when(cache.getPersistentMemberManager()).thenReturn(persistentMemberManager);

    showMissingDiskStoresFunction.execute(functionContext);

    assertThat(resultSender.getResults())
        .as("results collection")
        .hasSize(1);

    assertThat(resultSender.getResults().get(0))
        .as("results element [0]: null")
        .isNull();
  }

  @Test
  public void execute_throwsRuntimeException_whenFunctionContextIsNull() {
    Throwable thrown = catchThrowable(() -> showMissingDiskStoresFunction.execute(null));

    // NOTE: throwing RuntimeException with no message is a bad practice
    assertThat(thrown)
        .as("throwable thrown by execute")
        .isInstanceOf(RuntimeException.class)
        .hasMessage(null);
  }

  @Test
  public void execute_resultsContains_null_whenCacheIsClosed() {
    when(cache.isClosed()).thenReturn(true);

    showMissingDiskStoresFunction.execute(functionContext);

    assertThat(resultSender.getResults())
        .as("results collection")
        .hasSize(1);

    assertThat(resultSender.getResults().get(0))
        .as("results element [0]: null")
        .isNull();
  }

  @Test
  public void execute_resultsContains_MissingDiskStores() throws UnknownHostException {
    PersistentMemberID persistentMemberId1 = persistentMemberID("/directory1", 1, (short) 1);
    PersistentMemberID persistentMemberId2 = persistentMemberID("/directory2", 2, (short) 2);
    Map<String, Set<PersistentMemberID>> waitingRegions = new HashMap<>();
    waitingRegions.put("region1", new HashSet<>(asList(persistentMemberId1, persistentMemberId2)));

    when(cache.getPersistentMemberManager()).thenReturn(persistentMemberManager);
    when(persistentMemberManager.getWaitingRegions()).thenReturn(waitingRegions);

    showMissingDiskStoresFunction.execute(functionContext);

    List<Set<?>> results = getResults(functionContext.getResultSender());
    assertThat(results)
        .as("results collection")
        .hasSize(1);

    assertThat(missingDiskStores(results.get(0)))
        .as("results element [0]: missingDiskStores")
        .hasSize(2)
        .containsExactlyInAnyOrder(
            new PersistentMemberPattern(persistentMemberId1),
            new PersistentMemberPattern(persistentMemberId2));
  }

  @Test
  public void execute_resultsContains_MissingColocatedRegions() {
    InternalDistributedMember member = distributedMember("host1", "name1");

    when(cache.getMyId()).thenReturn(member);
    when(cache.getPartitionedRegions()).thenReturn(asSet(region1, region2));
    when(cache.getPersistentMemberManager()).thenReturn(persistentMemberManager);
    when(region1.getFullPath()).thenReturn("/pr1");
    when(region1.getMissingColocatedChildren()).thenReturn(asList("child1", "child2"));

    showMissingDiskStoresFunction.execute(functionContext);

    List<Set<?>> results = getResults(functionContext.getResultSender());
    assertThat(results)
        .as("results collection")
        .hasSize(1);

    assertThat(missingColocatedRegions(results.get(0)))
        .as("results element [0]: missingColocatedRegions")
        .hasSize(2)
        .containsExactlyInAnyOrder(
            new ColocatedRegionDetails("host1", "name1", "/pr1", "child1"),
            new ColocatedRegionDetails("host1", "name1", "/pr1", "child2"));
  }

  @Test
  public void execute_resultsContains_missingDiskStores_andMissingColocatedRegions()
      throws UnknownHostException {
    InternalDistributedMember member = distributedMember("host2", "name2");
    PersistentMemberID persistentMemberId1 = persistentMemberID("/directory1", 1, (short) 1);
    PersistentMemberID persistentMemberId2 = persistentMemberID("/directory2", 2, (short) 2);
    Map<String, Set<PersistentMemberID>> waitingRegions = new HashMap<>();
    waitingRegions.put("region2", asSet(persistentMemberId1, persistentMemberId2));

    when(cache.getMyId()).thenReturn(member);
    when(cache.getPartitionedRegions()).thenReturn(asSet(region1, region2));
    when(cache.getPersistentMemberManager()).thenReturn(persistentMemberManager);
    when(persistentMemberManager.getWaitingRegions()).thenReturn(waitingRegions);
    when(region2.getFullPath()).thenReturn("/pr2");
    when(region2.getMissingColocatedChildren()).thenReturn(asList("child1", "child2"));

    showMissingDiskStoresFunction.execute(functionContext);

    List<Set<?>> results = getResults(functionContext.getResultSender());
    assertThat(results)
        .as("results collection")
        .hasSize(2);

    // 1st element is set of missing disk stores
    assertThat(missingDiskStores(results.get(0)))
        .as("results element [0]: missingDiskStores")
        .hasSize(2)
        .containsExactlyInAnyOrder(new PersistentMemberPattern(persistentMemberId1),
            new PersistentMemberPattern(persistentMemberId2));

    // 2nd element is set of missing colocated regions
    assertThat(missingColocatedRegions(results.get(1)))
        .as("results element [1]: missingColocatedRegions")
        .hasSize(2)
        .containsExactlyInAnyOrder(
            new ColocatedRegionDetails("host2", "name2", "/pr2", "child1"),
            new ColocatedRegionDetails("host2", "name2", "/pr2", "child2"));
  }

  @Test
  public void execute_resultsContains_exceptionCaughtByFunction() {
    RuntimeException exceptionCaughtByFunction = new NullPointerException("message");

    when(cache.getPersistentMemberManager()).thenThrow(exceptionCaughtByFunction);

    showMissingDiskStoresFunction.execute(functionContext);

    assertThat(resultSender.getThrowable())
        .as("throwable thrown by execute")
        .isSameAs(exceptionCaughtByFunction);
  }

  @Test
  public void getId_returnsFullyQualifiedClassName() {
    assertThat(showMissingDiskStoresFunction.getId())
        .as("function id")
        .isEqualTo(ShowMissingDiskStoresFunction.class.getName());
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static List<Set<?>> getResults(ResultSender<?> resultSender) {
    return ((CollectingResultSender) resultSender).getResults();
  }

  @SuppressWarnings("unchecked")
  private static Set<PersistentMemberPattern> missingDiskStores(Set<?> results) {
    return (Set<PersistentMemberPattern>) results;
  }

  @SuppressWarnings("unchecked")
  private static Set<ColocatedRegionDetails> missingColocatedRegions(Set<?> results) {
    return (Set<ColocatedRegionDetails>) results;
  }

  private static PersistentMemberID persistentMemberID(String directory, long timeStamp,
      short version)
      throws UnknownHostException {
    return new PersistentMemberID(new DiskStoreID(randomUUID()), getLocalHost(), directory,
        timeStamp, version);
  }

  private static InternalDistributedMember distributedMember(String host, String name) {
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getHost()).thenReturn(host);
    when(member.getName()).thenReturn(name);
    return member;
  }

  @SafeVarargs
  private static <T> Set<T> asSet(T... values) {
    Set<T> set = new HashSet<>();
    addAll(set, values);
    return set;
  }

  private static class CollectingResultSender<T> implements ResultSender<Set<T>> {

    private final List<Set<T>> results = new CopyOnWriteArrayList<>();
    private final AtomicReference<Throwable> throwableRef = new AtomicReference<>();

    List<Set<T>> getResults() {
      return Collections.unmodifiableList(results);
    }

    Throwable getThrowable() {
      return throwableRef.get();
    }

    @Override
    public void lastResult(Set<T> lastResult) {
      results.add(lastResult);
    }

    @Override
    public void sendResult(Set<T> oneResult) {
      results.add(oneResult);
    }

    @Override
    public void sendException(Throwable t) {
      throwableRef.set(t);
    }
  }
}
