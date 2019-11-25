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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.CompiledSelect;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.LinkedResultSet;
import org.apache.geode.cache.query.internal.QueryConfigurationService;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.test.fake.Fakes;

public class PartitionedRegionQueryEvaluatorTest {
  private InternalDistributedMember localNode;
  private InternalDistributedMember remoteNodeA;
  private InternalDistributedMember remoteNodeB;
  private InternalDistributedSystem system;
  private PartitionedRegion pr;
  private DefaultQuery query;
  // Needed to help mock out certain scenarios
  private ExtendedPartitionedRegionDataStore dataStore;
  // This is the set of nodes that remain after a failure
  private ArrayList<InternalDistributedMember> allNodes = new ArrayList<>();
  // convenience list for empty set
  private Set<InternalDistributedMember> noFailingMembers = new HashSet<>();

  @Before
  public void setup() throws Exception {
    QueryConfigurationService mockService = mock(QueryConfigurationService.class);
    when(mockService.getMethodAuthorizer()).thenReturn(mock(MethodInvocationAuthorizer.class));

    localNode = new InternalDistributedMember("localhost", 8888);
    remoteNodeA = new InternalDistributedMember("localhost", 8889);
    remoteNodeB = new InternalDistributedMember("localhost", 8890);
    GemFireCacheImpl cache = Fakes.cache();
    system = (InternalDistributedSystem) cache.getDistributedSystem();
    when(cache.getService(QueryConfigurationService.class)).thenReturn(mockService);

    allNodes.add(localNode);
    allNodes.add(remoteNodeA);
    allNodes.add(remoteNodeB);

    pr = mock(PartitionedRegion.class);

    when(pr.getCache()).thenReturn(cache);
    dataStore = new ExtendedPartitionedRegionDataStore();
    CompiledSelect select = mock(CompiledSelect.class);
    when(select.getType()).thenReturn(CompiledValue.COMPARISON);
    when(select.getElementTypeForOrderByQueries()).thenReturn(new ObjectTypeImpl(String.class));
    query = mock(DefaultQuery.class);
    when(query.getSimpleSelect()).thenReturn(select);
    when(query.getLimit(any())).thenReturn(-1);
    when(pr.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
    when(pr.getMyId()).thenReturn(localNode);
    when(pr.getDataStore()).thenReturn(dataStore);
    when(pr.getCache()).thenReturn(cache);
  }

  @Test
  public void testLocalQueryReturnsResultsToPartitionedQueryEvaluator() throws Exception {
    List resultsForMember1 = createResultObjects("1", "2", "3");

    PartitionedQueryScenario scenario = new PartitionedQueryScenario(localNode, allNodes,
        noFailingMembers, createFakeBucketMap(), new ProcessDataFaker() {
          @Override
          public void processData(PartitionedRegionQueryEvaluator prqe) {
            // this test won't have any remote nodes responding
          }

          @Override
          @SuppressWarnings("unchecked")
          public void executeQueryLocally(Collection resultsCollector) {
            resultsCollector.add(resultsForMember1);
          }
        });

    Set<Integer> allBucketsToQuery = scenario.getAllBucketsToQuery();
    Queue<PartitionedQueryScenario> scenarios = createScenariosQueue(scenario);
    dataStore.setScenarios(scenarios);

    PartitionedRegionQueryEvaluator prqe = new ExtendedPartitionedRegionQueryEvaluator(system, pr,
        query, mock(ExecutionContext.class), null, new LinkedResultSet(), allBucketsToQuery,
        scenarios);

    Collection results = prqe.queryBuckets(null).asList();
    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(resultsForMember1.size());
    results.removeAll(resultsForMember1);
    assertThat(results.isEmpty()).isTrue();
  }

  @Test
  public void testRemoteAndLocalQueryReturnsResultsToPartitionedQueryEvaluator() throws Exception {
    List<Object> resultsForMember1 = createResultObjects("1", "2", "3");
    List<Object> resultsForMember2 = createResultObjects("4", "5", "6");

    PartitionedQueryScenario scenario = new PartitionedQueryScenario(localNode, allNodes,
        noFailingMembers, createFakeBucketMap(), new ProcessDataFaker() {
          @Override
          public void processData(PartitionedRegionQueryEvaluator prqe) {
            prqe.processData(resultsForMember2, remoteNodeA, 0, true);
          }

          @Override
          @SuppressWarnings("unchecked")
          public void executeQueryLocally(Collection resultsCollector) {
            resultsCollector.add(resultsForMember1);
          }
        });

    Set<Integer> allBucketsToQuery = scenario.getAllBucketsToQuery();
    Queue<PartitionedQueryScenario> scenarios = createScenariosQueue(scenario);
    dataStore.setScenarios(scenarios);

    PartitionedRegionQueryEvaluator prqe = new ExtendedPartitionedRegionQueryEvaluator(system, pr,
        query, mock(ExecutionContext.class), null, new LinkedResultSet(), allBucketsToQuery,
        scenarios);
    Collection results = prqe.queryBuckets(null).asList();

    List<Object> expectedResults = new LinkedList();
    expectedResults.addAll(resultsForMember1);
    expectedResults.addAll(resultsForMember2);
    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(expectedResults.size());
    results.removeAll(expectedResults);
    assertThat(results.isEmpty()).isTrue();
  }


  @Test
  public void testFailingRemoteNodeAndRetryOnLocalNodeDoesNotSquashResultsOfOriginalQueryOnLocalNode()
      throws Exception {
    List resultsForMember1 = createResultObjects("1", "2", "3");
    List resultsForMember2 = createResultObjects("A", "B", "C");
    List resultsForMember1ForRetry = createResultObjects("&", "$", "!");

    Set<InternalDistributedMember> failingMembers = new HashSet<>();
    failingMembers.add(remoteNodeB);

    PartitionedQueryScenario allNodesUpAtBeginning = new PartitionedQueryScenario(localNode,
        allNodes, failingMembers, createFakeBucketMap(), new ProcessDataFaker() {
          @Override
          public void processData(PartitionedRegionQueryEvaluator prqe) {
            prqe.processData(resultsForMember2, remoteNodeA, 0, true);
          }

          @Override
          @SuppressWarnings("unchecked")
          public void executeQueryLocally(Collection resultsCollector) {
            resultsCollector.add(resultsForMember1);
          }
        });

    PartitionedQueryScenario afterFailureScenario =
        new PartitionedQueryScenario(localNode, allNodes, noFailingMembers,
            createFakeBucketMapFailedNodesToLocalMember(), new ProcessDataFaker() {
              @Override
              public void processData(PartitionedRegionQueryEvaluator prqe) {
                // on retry we do not need to fake a retry on a remote node for this test
              }

              @Override
              @SuppressWarnings("unchecked")
              public void executeQueryLocally(Collection resultsCollector) {
                resultsCollector.add(resultsForMember1);
              }
            });

    Set<Integer> allBucketsToQuery = allNodesUpAtBeginning.getAllBucketsToQuery();
    Queue<PartitionedQueryScenario> scenarios =
        createScenariosQueue(allNodesUpAtBeginning, afterFailureScenario);
    dataStore.setScenarios(scenarios);

    PartitionedRegionQueryEvaluator prqe = new ExtendedPartitionedRegionQueryEvaluator(system, pr,
        query, mock(ExecutionContext.class), null, new LinkedResultSet(), allBucketsToQuery,
        scenarios);
    Collection results = prqe.queryBuckets(null).asList();

    List<Object> expectedResults = new LinkedList<>();
    expectedResults.addAll(resultsForMember1);
    expectedResults.addAll(resultsForMember2);
    expectedResults.addAll(resultsForMember1ForRetry);
    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(expectedResults.size());
    results.removeAll(expectedResults);
    assertThat(results.isEmpty()).isTrue();
  }

  @Test
  public void testGetAllNodesShouldBeRandomized() {
    List<Integer> bucketList = createBucketList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    Set bucketSet = new HashSet<>(bucketList);
    PartitionedRegionQueryEvaluator prqe =
        new PartitionedRegionQueryEvaluator(system, pr, query, mock(ExecutionContext.class),
            null, new LinkedResultSet(), bucketSet);
    RegionAdvisor regionAdvisor = mock(RegionAdvisor.class);
    when(regionAdvisor.adviseDataStore()).thenReturn(bucketSet);
    await()
        .until(() -> !(bucketList.equals(prqe.getAllNodes(regionAdvisor))));
  }

  private Map<InternalDistributedMember, List<Integer>> createFakeBucketMap() {
    Map<InternalDistributedMember, List<Integer>> bucketToNodeMap = new HashMap<>();
    bucketToNodeMap.put(localNode, createBucketList(1, 2, 3));
    bucketToNodeMap.put(remoteNodeA, createBucketList(4, 5, 6));
    bucketToNodeMap.put(remoteNodeB, createBucketList(7, 8, 9));
    return bucketToNodeMap;
  }

  // fake bucket map to use after we fake a node failure
  private Map<InternalDistributedMember, List<Integer>> createFakeBucketMapFailedNodesToLocalMember() {
    Map<InternalDistributedMember, List<Integer>> bucketToNodeMap = new HashMap<>();
    bucketToNodeMap.put(localNode, createBucketList(1, 2, 3, 7, 8, 9));
    bucketToNodeMap.put(remoteNodeA, createBucketList(4, 5, 6));
    return bucketToNodeMap;
  }

  private List<Integer> createBucketList(int... buckets) {
    List<Integer> bucketList = new ArrayList<>();
    for (int i : buckets) {
      bucketList.add(i);
    }
    return bucketList;
  }

  private List<Object> createResultObjects(Object... resultObjects) {
    List<Object> results = new LinkedList<>();
    Collections.addAll(results, resultObjects);
    return results;
  }

  private Queue<PartitionedQueryScenario> createScenariosQueue(
      PartitionedQueryScenario... scenarios) {
    Queue<PartitionedQueryScenario> queue = new LinkedList<>();
    Collections.addAll(queue, scenarios);
    return queue;
  }

  private static class ExtendedPartitionedRegionDataStore extends PartitionedRegionDataStore {
    // Must be the same referenced queue as that used by the ExtendedPartitionedRegionQueryEvaluator
    // That way they will be synched to the same scenario;
    Queue<PartitionedQueryScenario> scenarios;

    void setScenarios(Queue<PartitionedQueryScenario> scenarios) {
      this.scenarios = scenarios;
    }

    @Override
    public boolean isManagingBucket(int bucketId) {
      PartitionedQueryScenario scenario = scenarios.peek();
      assertThat(scenario).isNotNull();

      return scenario.isLocalManagingBucket(bucketId);
    }
  }

  private static class ExtendedPartitionedRegionQueryEvaluator
      extends PartitionedRegionQueryEvaluator {

    Queue<PartitionedQueryScenario> scenarios;

    // pass through so we can fake out the executeQuery locally
    PRQueryProcessor extendedPRQueryProcessor;

    ExtendedPartitionedRegionQueryEvaluator(final InternalDistributedSystem sys,
        final PartitionedRegion pr,
        final DefaultQuery query,
        final ExecutionContext executionContext,
        final Object[] parameters,
        final SelectResults cumulativeResults,
        final Set<Integer> bucketsToQuery,
        final Queue<PartitionedQueryScenario> scenarios) {
      super(sys, pr, query, executionContext, parameters, cumulativeResults, bucketsToQuery);
      this.scenarios = scenarios;
      extendedPRQueryProcessor =
          new ExtendedPRQueryProcessor(pr, query, null, new LinkedList<>(bucketsToQuery));
    }

    private PartitionedQueryScenario currentScenario() {
      return scenarios.peek();
    }

    // (package access for unit test purposes)
    @Override
    Map<InternalDistributedMember, List<Integer>> buildNodeToBucketMap() {
      return currentScenario().bucketMap;
    }

    @Override
    protected StreamingQueryPartitionResponse createStreamingQueryPartitionResponse(
        InternalDistributedSystem system, HashMap<InternalDistributedMember, List<Integer>> n2b) {
      return new FakeNumFailStreamingQueryPartitionResponse(system, n2b, this, scenarios);
    }

    @Override
    protected Set sendMessage(DistributionMessage m) {
      // Don't need to actually send the message...
      return null;
    }

    @Override
    protected PRQueryProcessor createLocalPRQueryProcessor(List<Integer> bucketList) {
      return extendedPRQueryProcessor;
    }

    @Override
    protected ArrayList getAllNodes(RegionAdvisor advisor) {
      return currentScenario().allNodes;
    }

    @Override
    protected Set<InternalDistributedMember> getBucketOwners(Integer bid) {
      return currentScenario().getBucketOwners(bid);
    }

    private class ExtendedPRQueryProcessor extends PRQueryProcessor {

      ExtendedPRQueryProcessor(PartitionedRegion pr, DefaultQuery query, Object[] parameters,
          List buckets) {
        super(pr, query, parameters, buckets);
      }

      @Override
      public boolean executeQuery(Collection resultsCollector) {
        currentScenario().processDataFaker.executeQueryLocally(resultsCollector);
        return true;
      }
    }

    private class FakeNumFailStreamingQueryPartitionResponse
        extends StreamingQueryPartitionResponse {
      private PartitionedRegionQueryEvaluator processor;
      Queue<PartitionedQueryScenario> scenarios;

      FakeNumFailStreamingQueryPartitionResponse(InternalDistributedSystem system,
          HashMap<InternalDistributedMember, List<Integer>> n2b,
          PartitionedRegionQueryEvaluator processor,
          Queue<PartitionedQueryScenario> scenarios) {
        super(system, n2b.keySet());
        this.processor = processor;
        this.scenarios = scenarios;
      }

      @Override
      public Set<InternalDistributedMember> waitForCacheOrQueryException()
          throws CacheException {
        currentScenario().processDataFaker.processData(processor);
        Set<InternalDistributedMember> returnValue = currentScenario().failingNodes;
        advanceTheScenario();
        return returnValue;
      }

      private void advanceTheScenario() {
        if (scenarios.isEmpty()) {
          return;
        }
        scenarios.remove();
      }
    }
  }

  private interface ProcessDataFaker {
    void processData(PartitionedRegionQueryEvaluator processor);

    void executeQueryLocally(Collection resultsCollector);
  }

  // holds information on how the PRQE is to behave and what responses are "returned"
  private static class PartitionedQueryScenario {
    private InternalDistributedMember localNode;
    private ArrayList allNodes;
    private Set<InternalDistributedMember> failingNodes;
    private ProcessDataFaker processDataFaker;
    private Map<InternalDistributedMember, List<Integer>> bucketMap;

    PartitionedQueryScenario(InternalDistributedMember localNode, ArrayList allNodes,
        Set<InternalDistributedMember> failingNodes,
        Map<InternalDistributedMember, List<Integer>> bucketMap,
        ProcessDataFaker processDataFaker) {
      this.localNode = localNode;
      this.allNodes = allNodes;
      this.failingNodes = failingNodes;
      this.bucketMap = bucketMap;
      this.processDataFaker = processDataFaker;
    }

    Set<Integer> getAllBucketsToQuery() {
      Set<Integer> allBuckets = new HashSet<>();
      bucketMap.values().forEach(allBuckets::addAll);

      return allBuckets;
    }

    Set<InternalDistributedMember> getBucketOwners(Integer bucketId) {
      Set<InternalDistributedMember> owners = new HashSet<>();
      bucketMap.forEach((key, value) -> {
        if (value.contains(bucketId)) {
          owners.add(key);
        }
      });
      return owners;
    }

    boolean isLocalManagingBucket(int bucketId) {
      return getBucketOwners(bucketId).contains(localNode);
    }
  }
}
