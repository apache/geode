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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.DefaultQuery.TestHook;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Distributed tests for querying a PartitionedRegion.
 */
@SuppressWarnings("serial")
public class PRQueryDistributedTest implements Serializable {

  private String regionName;

  private VM vm0;
  private VM vm1;
  private VM vm2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);

    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  @After
  public void tearDown() {
    invokeInEveryVM(() -> {
      DefaultQuery.QUERY_VERBOSE = false;
      DefaultQuery.testHook = null;

      // tearDown for IndexManager.setIndexBufferTime
      IndexManager.SAFE_QUERY_TIME.set(0);
    });
  }

  @Test
  public void testReevaluationDueToUpdateInProgress() throws Exception {
    vm0.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createPartitionedRegion());

    vm0.invoke(() -> createIndex("compactRangeIndex", "entry.value",
        SEPARATOR + regionName + ".entrySet entry"));

    vm0.invoke("putting data", () -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      for (int i = 0; i < 100; i++) {
        region.put(i, new TestObject(i));
      }
    });

    vm0.invoke("resetting sqt", () -> {
      IndexManager.setIndexBufferTime(Long.MAX_VALUE, Long.MAX_VALUE);
    });
    vm1.invoke("resetting sqt", () -> {
      IndexManager.setIndexBufferTime(Long.MAX_VALUE, Long.MAX_VALUE);
    });

    vm0.invoke("query", () -> {
      cacheRule.getCache().getQueryService()
          .newQuery("SELECT DISTINCT entry.key, entry.value FROM " + SEPARATOR + regionName
              + ".entrySet entry WHERE entry.value.score >= 5 AND entry.value.score <= 10 ORDER BY value asc")
          .execute();
    });
  }

  /**
   * Tests trace for PR queries when {@code <trace>} is used and query verbose is set to true on
   * local and remote servers
   */
  @Test
  public void testPartitionRegionDebugMessageQueryTraceOnBothServers() throws Exception {
    vm0.invoke(() -> createAccessor());
    vm1.invoke(() -> createPartitionedRegion());
    vm2.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createBuckets());

    vm1.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = true;
    });
    vm2.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = true;
    });

    vm1.invoke(() -> {
      Query query = cacheRule.getCache().getQueryService()
          .newQuery("<trace> select * from " + SEPARATOR + regionName + " r where r > 0");
      SelectResults results = (SelectResults) query.execute();
      assertThat(results.asSet()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
    });

    vm1.invoke(() -> {
      PRQueryTraceTestHook server1TestHookInVM1 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server1TestHookInVM1.getHooks().get(TestHook.SPOTS.PULL_OFF_PR_QUERY_TRACE_INFO))
          .isTrue();
      assertThat(server1TestHookInVM1.getHooks().get(TestHook.SPOTS.CREATE_PR_QUERY_TRACE_STRING))
          .isTrue();
      assertThat(server1TestHookInVM1.getHooks()
          .get(TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FROM_LOCAL_NODE))
              .isTrue();
    });
    vm2.invoke(() -> {
      PRQueryTraceTestHook server2TestHookInVM2 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.POPULATING_TRACE_INFO_FOR_REMOTE_QUERY))
              .isTrue();
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FOR_REMOTE_QUERY))
              .isTrue();
    });
  }

  /**
   * Tests trace for PR queries when {@code <trace>} is used and query verbose is set to true on
   * local but false on remote servers All flags should be true still as the {@code <trace>} is OR'd
   * with query verbose flag
   */
  @Test
  public void testPartitionRegionDebugMessageQueryTraceOnLocalServerOnly() throws Exception {
    vm0.invoke(() -> createAccessor());
    vm1.invoke(() -> createPartitionedRegion());
    vm2.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createBuckets());

    vm1.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = true;
    });
    vm2.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = false;
    });

    vm1.invoke(() -> {
      Query query = cacheRule.getCache().getQueryService()
          .newQuery("<trace> select * from " + SEPARATOR + regionName + " r where r > 0");
      SelectResults results = (SelectResults) query.execute();
      assertThat(results.asSet()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
    });

    vm1.invoke(() -> {
      PRQueryTraceTestHook server1TestHookInVM1 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server1TestHookInVM1.getHooks().get(TestHook.SPOTS.PULL_OFF_PR_QUERY_TRACE_INFO))
          .isTrue();
      assertThat(server1TestHookInVM1.getHooks().get(TestHook.SPOTS.CREATE_PR_QUERY_TRACE_STRING))
          .isTrue();
      assertThat(server1TestHookInVM1.getHooks()
          .get(TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FROM_LOCAL_NODE))
              .isTrue();
    });
    vm2.invoke(() -> {
      PRQueryTraceTestHook server2TestHookInVM2 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.POPULATING_TRACE_INFO_FOR_REMOTE_QUERY))
              .isTrue();
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FOR_REMOTE_QUERY))
              .isTrue();
    });
  }

  /**
   * Tests trace for PR queries when {@code <trace>} is NOT used and query verbose is set to true on
   * local but false on remote. The remote should not send a pr query trace info back because trace
   * was not requested.
   */
  @Test
  public void testPartitionRegionDebugMessageQueryTraceOffLocalServerVerboseOn() throws Exception {
    vm0.invoke(() -> createAccessor());
    vm1.invoke(() -> createPartitionedRegion());
    vm2.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createBuckets());

    vm1.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = true;
    });
    vm2.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = false;
    });

    vm1.invoke(() -> {
      Query query = cacheRule.getCache().getQueryService()
          .newQuery("select * from " + SEPARATOR + regionName + " r where r > 0");
      SelectResults results = (SelectResults) query.execute();
      assertThat(results.asSet()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
    });

    vm1.invoke(() -> {
      PRQueryTraceTestHook server1TestHookInVM1 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server1TestHookInVM1.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.PULL_OFF_PR_QUERY_TRACE_INFO)).isNull();
      assertThat(server1TestHookInVM1.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_STRING)).isTrue();
      assertThat(server1TestHookInVM1.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FROM_LOCAL_NODE))
              .isTrue();
    });
    vm2.invoke(() -> {
      PRQueryTraceTestHook server2TestHookInVM2 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.POPULATING_TRACE_INFO_FOR_REMOTE_QUERY))
              .isNull();
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FOR_REMOTE_QUERY))
              .isNull();
    });
  }

  /**
   * Tests trace for PR queries when {@code <trace>} is NOT used and query verbose is set to false
   * on local but true on remote servers We don't output the string or do anything on the local
   * side, but we still pull off the object due to the remote server generating and sending it
   * over.
   */
  @Test
  public void testPartitionRegionDebugMessageQueryTraceOffRemoteServerOnly() throws Exception {
    vm0.invoke(() -> createAccessor());
    vm1.invoke(() -> createPartitionedRegion());
    vm2.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createBuckets());

    vm1.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = false;
    });
    vm2.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = true;
    });

    vm1.invoke(() -> {
      Query query = cacheRule.getCache().getQueryService()
          .newQuery("select * from " + SEPARATOR + regionName + " r where r > 0");
      SelectResults results = (SelectResults) query.execute();
      assertThat(results.asSet()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
    });

    vm1.invoke(() -> {
      PRQueryTraceTestHook server1TestHookInVM1 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server1TestHookInVM1.getHooks()
          .get(TestHook.SPOTS.PULL_OFF_PR_QUERY_TRACE_INFO)).isTrue();
      assertThat(server1TestHookInVM1.getHooks()
          .get(TestHook.SPOTS.CREATE_PR_QUERY_TRACE_STRING)).isNull();
      assertThat(server1TestHookInVM1.getHooks()
          .get(TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FROM_LOCAL_NODE))
              .isNull();
    });
    vm2.invoke(() -> {
      PRQueryTraceTestHook server2TestHookInVM2 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.POPULATING_TRACE_INFO_FOR_REMOTE_QUERY))
              .isTrue();
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FOR_REMOTE_QUERY))
              .isTrue();
    });
  }

  /**
   * Tests trace for PR queries when {@code <trace>} is used and query verbose is set to false on
   * local and remote servers trace is OR'd so the entire trace process should be invoked.
   */
  @Test
  public void testPartitionRegionDebugMessageQueryTraceOnRemoteServerOnly() throws Exception {
    vm0.invoke(() -> createAccessor());
    vm1.invoke(() -> createPartitionedRegion());
    vm2.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createBuckets());

    vm1.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = false;
    });
    vm2.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = true;
    });

    vm1.invoke(() -> {
      Query query = cacheRule.getCache().getQueryService()
          .newQuery("<trace> select * from " + SEPARATOR + regionName + " r where r > 0");
      SelectResults results = (SelectResults) query.execute();
      assertThat(results.asSet()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
    });

    vm1.invoke(() -> {
      PRQueryTraceTestHook server1TestHookInVM1 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server1TestHookInVM1.getHooks().get(TestHook.SPOTS.PULL_OFF_PR_QUERY_TRACE_INFO))
          .isTrue();
      assertThat(server1TestHookInVM1.getHooks().get(TestHook.SPOTS.CREATE_PR_QUERY_TRACE_STRING))
          .isTrue();
      assertThat(server1TestHookInVM1.getHooks()
          .get(TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FROM_LOCAL_NODE))
              .isTrue();
    });
    vm2.invoke(() -> {
      PRQueryTraceTestHook server2TestHookInVM2 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.POPULATING_TRACE_INFO_FOR_REMOTE_QUERY))
              .isTrue();
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FOR_REMOTE_QUERY))
              .isTrue();
    });
  }

  /**
   * Tests trace for PR queries when {@code <trace>} is NOT used and query verbose is set to false
   * on local but true remote servers The local node still receives the pr trace info from the
   * remote node due to query verbose being on however nothing is used on the local side.
   */
  @Test
  public void testPartitionRegionDebugMessageQueryTraceOffRemoteServerOn() throws Exception {
    vm0.invoke(() -> createAccessor());
    vm1.invoke(() -> createPartitionedRegion());
    vm2.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createBuckets());

    vm1.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = false;
    });
    vm2.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = true;
    });

    vm1.invoke(() -> {
      Query query = cacheRule.getCache().getQueryService()
          .newQuery("select * from " + SEPARATOR + regionName + " r where r > 0");
      SelectResults results = (SelectResults) query.execute();
      assertThat(results.asSet()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
    });

    vm1.invoke(() -> {
      PRQueryTraceTestHook server1TestHookInVM1 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server1TestHookInVM1.getHooks()
          .get(TestHook.SPOTS.PULL_OFF_PR_QUERY_TRACE_INFO)).isTrue();
      assertThat(server1TestHookInVM1.getHooks()
          .get(TestHook.SPOTS.CREATE_PR_QUERY_TRACE_STRING)).isNull();
      assertThat(server1TestHookInVM1.getHooks()
          .get(TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FROM_LOCAL_NODE))
              .isNull();
    });
    vm2.invoke(() -> {
      PRQueryTraceTestHook server2TestHookInVM2 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.POPULATING_TRACE_INFO_FOR_REMOTE_QUERY))
              .isTrue();
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FOR_REMOTE_QUERY))
              .isTrue();
    });
  }

  /**
   * Tests trace for PR queries when {@code <trace>} is NOT used and query verbose is set to false
   * on local and remote servers None of our hooks should have triggered.
   */
  @Test
  public void testPartitionRegionDebugMessageQueryTraceOffQueryVerboseOff() throws Exception {
    vm0.invoke(() -> createAccessor());
    vm1.invoke(() -> createPartitionedRegion());
    vm2.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createBuckets());

    vm1.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = false;
    });
    vm2.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = false;
    });

    vm1.invoke(() -> {
      Query query = cacheRule.getCache().getQueryService()
          .newQuery("select * from " + SEPARATOR + regionName + " r where r > 0");
      SelectResults results = (SelectResults) query.execute();
      assertThat(results.asSet()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
    });

    vm1.invoke(() -> {
      PRQueryTraceTestHook server1TestHookInVM1 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server1TestHookInVM1.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.PULL_OFF_PR_QUERY_TRACE_INFO)).isNull();
      assertThat(server1TestHookInVM1.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_STRING)).isNull();
      assertThat(server1TestHookInVM1.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FROM_LOCAL_NODE))
              .isNull();
    });
    vm2.invoke(() -> {
      PRQueryTraceTestHook server2TestHookInVM2 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.POPULATING_TRACE_INFO_FOR_REMOTE_QUERY))
              .isNull();
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FOR_REMOTE_QUERY))
              .isNull();
    });
  }

  /**
   * Test trace for PR queries when {@code <trace>} is used and query verbose is set to false on
   * local and remote servers. All hooks should have triggered due to trace being used.
   */
  @Test
  public void testPartitionRegionDebugMessageQueryTraceOnQueryVerboseOff() throws Exception {
    vm0.invoke(() -> createAccessor());
    vm1.invoke(() -> createPartitionedRegion());
    vm2.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createBuckets());

    vm1.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = false;
    });
    vm2.invoke(() -> {
      DefaultQuery.testHook = new PRQueryTraceTestHook();
      DefaultQuery.QUERY_VERBOSE = false;
    });

    vm1.invoke(() -> {
      Query query = cacheRule.getCache().getQueryService()
          .newQuery("<trace> select * from " + SEPARATOR + regionName + " r where r > 0");
      SelectResults results = (SelectResults) query.execute();
      assertThat(results.asSet()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
    });

    vm1.invoke(() -> {
      PRQueryTraceTestHook server1TestHookInVM1 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server1TestHookInVM1.getHooks().get(TestHook.SPOTS.PULL_OFF_PR_QUERY_TRACE_INFO))
          .isTrue();
      assertThat(server1TestHookInVM1.getHooks().get(TestHook.SPOTS.CREATE_PR_QUERY_TRACE_STRING))
          .isTrue();
      assertThat(server1TestHookInVM1.getHooks()
          .get(TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FROM_LOCAL_NODE))
              .isTrue();
    });
    vm2.invoke(() -> {
      PRQueryTraceTestHook server2TestHookInVM2 = (PRQueryTraceTestHook) DefaultQuery.testHook;
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.POPULATING_TRACE_INFO_FOR_REMOTE_QUERY))
              .isTrue();
      assertThat(server2TestHookInVM2.getHooks()
          .get(DefaultQuery.TestHook.SPOTS.CREATE_PR_QUERY_TRACE_INFO_FOR_REMOTE_QUERY))
              .isTrue();
    });
  }

  private void createPartitionedRegion() {
    cacheRule.createCache();
    PartitionAttributesFactory paf = new PartitionAttributesFactory().setTotalNumBuckets(10);
    cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(paf.create()).create(regionName);
  }

  private void createAccessor() {
    cacheRule.createCache();
    PartitionAttributesFactory paf =
        new PartitionAttributesFactory().setTotalNumBuckets(10).setLocalMaxMemory(0);
    cacheRule.getCache().createRegionFactory(RegionShortcut.PARTITION_PROXY)
        .setPartitionAttributes(paf.create()).create(regionName);
  }

  private void createBuckets() {
    Region region = cacheRule.getCache().getRegion(regionName);
    for (int i = 0; i < 10; i++) {
      region.put(i, i);
    }
  }

  private void createIndex(String indexName, String indexedExpression, String regionPath)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    cacheRule.getCache().getQueryService().createIndex(indexName, indexedExpression, regionPath);
  }

  private static class TestObject implements DataSerializable, Comparable {

    private Double score;

    public TestObject() {
      // nothing
    }

    public TestObject(double score) {
      this.score = score;
    }

    @Override
    public int compareTo(Object o) {
      if (o instanceof TestObject) {
        return score.compareTo(((TestObject) o).score);
      }
      return 1;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeDouble(score);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      score = in.readDouble();
    }
  }

  private static class PRQueryTraceTestHook implements TestHook, Serializable {

    private final Map<SPOTS, Boolean> hooks = new HashMap<>();

    Map<SPOTS, Boolean> getHooks() {
      return hooks;
    }

    @Override
    public void doTestHook(final SPOTS spot, final DefaultQuery _ignored) {
      hooks.put(spot, Boolean.TRUE);
    }
  }
}
