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
package org.apache.geode.cache.query.internal;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.index.CompactRangeIndex;
import org.apache.geode.cache.query.internal.index.IndexProtocol;
import org.apache.geode.cache.query.internal.index.PartitionedIndex;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class QueryTraceJUnitTest {

  static QueryService qs;
  static Region region;
  static Index keyIndex1;

  private static final String queryStr = "select * from " + SEPARATOR + "portfolio where ID > 0";
  public static final int NUM_BKTS = 20;
  public static final String INDEX_NAME = "keyIndex1";

  private static File logfile;

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    DefaultQuery.testHook = new BeforeQueryExecutionHook();
  }

  @After
  public void tearDown() throws Exception {
    DefaultQuery.testHook = null;
    CacheUtils.closeCache();
  }

  /**
   * Tests tracing on queries with <TRACE> or <trace> tag.
   *
   */
  @Test
  public void testTraceOnPartitionedRegionWithTracePrefix() throws Exception {

    String slComment = "-- single line comment with TRACE \n";
    String mlComment =
        " /* Multi-line comments here" + "* ends here " + "* with TRACE too" + "*/ <TRACE> ";
    String prefix = slComment + mlComment;

    // Create Partition Region
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(NUM_BKTS);
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();

    keyIndex1 =
        (IndexProtocol) qs.createIndex(INDEX_NAME, IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof PartitionedIndex);

    Query query = qs.newQuery(prefix + queryStr);
    assertTrue(((DefaultQuery) query).isTraced());

    SelectResults results = (SelectResults) query.execute();

    // The IndexTrackingObserver should have been set
    BeforeQueryExecutionHook hook = (BeforeQueryExecutionHook) DefaultQuery.testHook;
    assertThat(hook.getObserver()).isInstanceOf(IndexTrackingQueryObserver.class);

    // The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();
  }

  @Test
  public void testTraceOnLocalRegionWithTracePrefix() throws Exception {

    String slComment = "-- single line comment with TRACE \n";
    String mlComment =
        " /* Multi-line comments here" + "* ends here " + "* with TRACE too" + "*/ <TRACE> ";
    String prefix = slComment + mlComment;

    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();

    keyIndex1 =
        (IndexProtocol) qs.createIndex(INDEX_NAME, IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    Query query = qs.newQuery(prefix + queryStr);
    assertTrue(((DefaultQuery) query).isTraced());

    SelectResults results = (SelectResults) query.execute();

    // The IndexTrackingObserver should have been set
    BeforeQueryExecutionHook hook = (BeforeQueryExecutionHook) DefaultQuery.testHook;
    assertThat(hook.getObserver()).isInstanceOf(IndexTrackingQueryObserver.class);

    // The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();
  }

  /**
   * negative testing: if <TRACE> is in comments not tracing is done.
   *
   */
  @Test
  public void testNegTraceOnPartitionedRegionWithTracePrefix() throws Exception {

    String slComment = "-- single line comment with TRACE \n";
    String mlComment = " /* Multi-line comments here" + "* ends here " + "* with TRACE too" + "*/";
    String prefix = slComment + mlComment;

    // Create Partition Region
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(NUM_BKTS);
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();

    keyIndex1 =
        (IndexProtocol) qs.createIndex(INDEX_NAME, IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof PartitionedIndex);

    Query query = qs.newQuery(prefix + queryStr);
    assertFalse(((DefaultQuery) query).isTraced());

    SelectResults results = (SelectResults) query.execute();

    // The IndexTrackingObserver should not have been set
    BeforeQueryExecutionHook hook = (BeforeQueryExecutionHook) DefaultQuery.testHook;
    assertThat(hook.getObserver()).isNotInstanceOf(IndexTrackingQueryObserver.class);

    // The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();
  }

  /**
   * negative testing: if <TRACE> is in comments not tracing is done.
   *
   */
  @Test
  public void testNegTraceOnLocalRegionWithTracePrefix() throws Exception {

    String slComment = "-- single line comment with TRACE \n";
    String mlComment = " /* Multi-line comments here" + "* ends here " + "* with TRACE too" + "*/";
    String prefix = slComment + mlComment;

    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();

    keyIndex1 =
        (IndexProtocol) qs.createIndex(INDEX_NAME, IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    Query query = qs.newQuery(prefix + queryStr);
    assertFalse(((DefaultQuery) query).isTraced());

    SelectResults results = (SelectResults) query.execute();

    // The IndexTrackingObserver should not have been set
    BeforeQueryExecutionHook hook = (BeforeQueryExecutionHook) DefaultQuery.testHook;
    assertThat(hook.getObserver()).isNotInstanceOf(IndexTrackingQueryObserver.class);

    // The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();
  }

  /**
   * No Query comments
   *
   */
  @Test
  public void testTraceOnPartitionedRegionWithTracePrefixNoComments() throws Exception {

    String prefix = "  <TRACE> ";
    // Create Partition Region
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(NUM_BKTS);
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();

    keyIndex1 =
        (IndexProtocol) qs.createIndex(INDEX_NAME, IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof PartitionedIndex);

    Query query = qs.newQuery(prefix + queryStr);
    assertTrue(((DefaultQuery) query).isTraced());

    SelectResults results = (SelectResults) query.execute();

    // The IndexTrackingObserver should have been set
    BeforeQueryExecutionHook hook = (BeforeQueryExecutionHook) DefaultQuery.testHook;
    assertThat(hook.getObserver()).isInstanceOf(IndexTrackingQueryObserver.class);

    // The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();
  }

  @Test
  public void testTraceOnLocalRegionWithTracePrefixNoComments() throws Exception {

    String prefix = "  <TRACE> ";

    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();

    keyIndex1 =
        (IndexProtocol) qs.createIndex(INDEX_NAME, IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    Query query = qs.newQuery(prefix + queryStr);
    assertTrue(((DefaultQuery) query).isTraced());

    SelectResults results = (SelectResults) query.execute();

    // The IndexTrackingObserver should have been set
    BeforeQueryExecutionHook hook = (BeforeQueryExecutionHook) DefaultQuery.testHook;
    assertThat(hook.getObserver()).isInstanceOf(IndexTrackingQueryObserver.class);

    // The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();
  }

  @Test
  public void testTraceOnPartitionedRegionWithSmallTracePrefixNoComments() throws Exception {

    String prefix = "<trace> ";
    // Create Partition Region
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(NUM_BKTS);
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();

    keyIndex1 =
        (IndexProtocol) qs.createIndex(INDEX_NAME, IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof PartitionedIndex);

    Query query = qs.newQuery(prefix + queryStr);
    assertTrue(((DefaultQuery) query).isTraced());

    SelectResults results = (SelectResults) query.execute();

    // The IndexTrackingObserver should have been set
    BeforeQueryExecutionHook hook = (BeforeQueryExecutionHook) DefaultQuery.testHook;
    assertThat(hook.getObserver()).isInstanceOf(IndexTrackingQueryObserver.class);

    // The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();

  }

  @Test
  public void testTraceOnLocalRegionWithSmallTracePrefixNoComments() throws Exception {

    String prefix = "<trace> ";

    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();

    keyIndex1 =
        (IndexProtocol) qs.createIndex(INDEX_NAME, IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    Query query = qs.newQuery(prefix + queryStr);
    assertTrue(((DefaultQuery) query).isTraced());

    SelectResults results = (SelectResults) query.execute();

    // The IndexTrackingObserver should have been set
    BeforeQueryExecutionHook hook = (BeforeQueryExecutionHook) DefaultQuery.testHook;
    assertThat(hook.getObserver()).isInstanceOf(IndexTrackingQueryObserver.class);

    // The query should return all elements in region.
    assertEquals(region.size(), results.size());
    QueryObserverHolder.reset();
  }

  @Test
  public void testQueryFailLocalRegionWithSmallTraceSuffixNoComments() throws Exception {

    String suffix = "<trace> ";

    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();

    keyIndex1 =
        (IndexProtocol) qs.createIndex(INDEX_NAME, IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    try {
      Query query = qs.newQuery(queryStr + suffix);
    } catch (Exception e) {
      if (!(e instanceof QueryInvalidException)) {
        fail("Test Failed: Query is invalid but exception was not thrown!");
      }
    }
  }

  @Test
  public void testQueryFailLocalRegionWithSmallTracePrefixNoSpace() throws Exception {

    String prefix = "<trace>";

    // Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();

    keyIndex1 =
        (IndexProtocol) qs.createIndex(INDEX_NAME, IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    try {
      Query query = qs.newQuery(prefix + queryStr);
    } catch (Exception e) {
      if (!(e instanceof QueryInvalidException)) {
        fail("Test Failed: Query is invalid but exception was not thrown!");
      }
    }
  }

  private class BeforeQueryExecutionHook implements DefaultQuery.TestHook {
    private QueryObserver observer = null;

    @Override
    public void doTestHook(final SPOTS spot, final DefaultQuery _ignored,
        final ExecutionContext executionContext) {
      switch (spot) {
        case BEFORE_QUERY_EXECUTION:
          observer = executionContext.getObserver();
          break;
      }
    }

    public QueryObserver getObserver() {
      return observer;
    }
  }
}
