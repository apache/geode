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
/*
 * QueryServiceJUnitTest.java JUnit based test
 *
 * Created on March 8, 2005, 4:53 PM
 */
package org.apache.geode.cache.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class QueryServiceJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for (int i = 0; i < 5; i++) {
      region.put(i + "", new Portfolio(i));
    }
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  // tests to make sure no exception is thrown when a valid query is created
  // and an invalid query throws an exception
  @Test
  public void testNewQuery() {
    CacheUtils.log("testNewQuery");
    QueryService qs = CacheUtils.getQueryService();
    qs.newQuery("SELECT DISTINCT * FROM /root");

    try {
      qs.newQuery("SELET DISTINCT * FROM /root");
      fail("Should have thrown an InvalidQueryException");
    } catch (QueryInvalidException e) {
      // pass
    }
  }

  @Test
  public void toDateWithPresetDateShouldExecuteWithoutExceptions() throws Exception {
    String testDate = "01/01/2000";
    QueryService queryService = CacheUtils.getQueryService();
    Query query = queryService.newQuery(
        "SELECT * FROM /Portfolios WHERE createDate >= to_date('" + testDate + "', 'MM/dd/yyyy')");
    query.execute();
  }

  @Test
  public void toDateWithValidBindParameterDateShouldExecuteWithoutExceptions() throws Exception {
    String testDate = "01/01/2000";
    QueryService queryService = CacheUtils.getQueryService();
    Query query = queryService
        .newQuery("SELECT * FROM /Portfolios WHERE createDate >= to_date($1, 'MM/dd/yyyy')");
    query.execute(testDate);
  }

  @Test
  public void toDateWithInValidStringBindParameterDateShouldThrowQueryInvalidException()
      throws Exception {
    String invalid = "someInvalidString";
    QueryService queryService = CacheUtils.getQueryService();
    Query query = queryService
        .newQuery("SELECT * FROM /Portfolios WHERE createDate >= to_date($1, 'MM/dd/yyyy')");
    try {
      query.execute(invalid);
      fail();
    } catch (QueryInvalidException e) {
      // expected
    }
  }

  @Test
  public void toDateWithRegionAsBindParameterDateShouldThrowQueryInvalidException()
      throws Exception {
    Object invalid = CacheUtils.getRegion("Portfolios");
    QueryService queryService = CacheUtils.getQueryService();
    Query query = queryService
        .newQuery("SELECT * FROM /Portfolios WHERE createDate >= to_date($1, 'MM/dd/yyyy')");
    try {
      query.execute(invalid);
      fail();
    } catch (QueryInvalidException e) {
      // expected
    }
  }

  @Test
  public void testCreateIndex() throws Exception {
    CacheUtils.log("testCreateIndex");
    QueryService qs = CacheUtils.getQueryService();
    Index index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/Portfolios");

    try {
      index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/Portfolios");
      if (index != null) {
        fail("QueryService.createIndex allows duplicate index names");
      }
    } catch (IndexNameConflictException e) {
    }

    try {
      index = qs.createIndex("statusIndex1", IndexType.FUNCTIONAL, "status", "/Portfolios");
      if (index != null) {
        fail("QueryService.createIndex allows duplicate indexes");
      }
    } catch (IndexExistsException e) {
    }
  }

  @Test
  public void testIndexDefinitions() {
    Object[][] testDataFromClauses = {{"status", "/Portfolios", Boolean.TRUE},
        {"status", "/Portfolios.entries", Boolean.FALSE},
        {"status", "/Portfolios.values", Boolean.TRUE},
        {"status", "/Portfolios.keys", Boolean.TRUE}, {"status", "/Portfolios p", Boolean.TRUE},
        {"status", "/Portfolio", Boolean.FALSE}, {"status", "/Portfolio.positions", Boolean.FALSE},
        {"status", "/Portfolios p, p.positions", Boolean.TRUE},};

    runCreateIndexTests(testDataFromClauses);

    Object[][] testDataIndexExpr = {{"positions", "/Portfolios", Boolean.FALSE},
        {"status.length", "/Portfolios", Boolean.TRUE}, {"p.status", "/Portfolios p", Boolean.TRUE},
        {"p.getStatus()", "/Portfolios p", Boolean.TRUE},
        {"pos.value.secId", "/Portfolios p, p.positions pos", Boolean.TRUE},
        {"pos.getValue().getSecId()", "/Portfolios p, p.positions pos", Boolean.TRUE},
        {"pos.getValue.secId", "/Portfolios p, p.positions pos", Boolean.TRUE},
        {"secId", "/Portfolios p, p.positions", Boolean.FALSE},
        {"is_defined(status)", "/Portfolios", Boolean.FALSE},
        {"is_undefined(status)", "/Portfolios", Boolean.FALSE},
        {"NOT(status = null)", "/Portfolios", Boolean.FALSE},
        {"$1", "/Portfolios", Boolean.FALSE},};

    runCreateIndexTests(testDataIndexExpr);
  }

  private void runCreateIndexTests(Object testData[][]) {
    QueryService qs = CacheUtils.getQueryService();
    qs.removeIndexes();
    for (int i = 0; i < testData.length; i++) {
      Index index = null;
      try {
        String indexedExpr = (String) testData[i][0];
        String fromClause = (String) testData[i][1];
        index = qs.createIndex("index" + i, IndexType.FUNCTIONAL, indexedExpr, fromClause);
        if (testData[i][2] == Boolean.TRUE && index == null) {
          fail("QueryService.createIndex unable to  create index for indexExpr=" + testData[i][0]
              + " from=" + testData[i][1]);
        } else if (testData[i][2] == Boolean.FALSE && index != null) {
          fail(
              "QueryService.createIndex allows to create index for un-supported index definition (indexExpr="
                  + testData[i][0] + " from=" + testData[i][1] + ")");
        }
      } catch (Exception e) {
        if (testData[i][2] == Boolean.TRUE) {
          e.printStackTrace();
          fail("QueryService.createIndex unable to  create index for indexExpr=" + testData[i][0]
              + " from=" + testData[i][1]);
        }
      } finally {
        if (index != null) {
          qs.removeIndex(index);
        }
      }
    }
  }

  @Ignore
  @Test
  public void testGetIndex() throws Exception {
    CacheUtils.log("testGetIndex");
    QueryService qs = CacheUtils.getQueryService();

    Region r = CacheUtils.getRegion("/Portfolios");
    Index index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/Portfolios");
    assertNotNull(qs.getIndex(r, "statusIndex"));
    qs.removeIndex(index);
    index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "p.status", "/Portfolios p");
    assertNotNull(qs.getIndex(r, "statusIndex"));
    qs.removeIndex(index);
    index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/Portfolios.values");
    assertNotNull(qs.getIndex(r, "statusIndex"));
    qs.removeIndex(index);
    index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "p.status", "/Portfolios.values p");
    assertNotNull(qs.getIndex(r, "statusIndex"));
    qs.removeIndex(index);
  }

  @Test
  public void testRemoveIndex() throws Exception {
    CacheUtils.log("testRemoveIndex");
    QueryService qs = CacheUtils.getQueryService();
    Index index = qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "p.status", "/Portfolios p");
    qs.removeIndex(index);
    index = qs.getIndex(CacheUtils.getRegion("/Portfolios"), "statusIndex");
    if (index != null) {
      fail("QueryService.removeIndex is not removing index");
    }
  }

  @Test
  public void testRemoveIndexes() throws Exception {
    CacheUtils.log("testRemoveIndexes");
    CacheUtils.createRegion("Ptfs", Portfolio.class);
    CacheUtils.createRegion("Ptfs1", Portfolio.class);
    QueryService qs = CacheUtils.getQueryService();
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/Portfolios");
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/Ptfs");
    qs.removeIndexes();
    Collection allIndexes = qs.getIndexes();
    if (allIndexes.size() != 0) {
      fail("QueryService.removeIndexes() does not removes all indexes");
    }
  }

  @Test
  public void testGetIndexes() throws Exception {
    CacheUtils.log("testGetIndexes");
    CacheUtils.createRegion("Ptfs", Portfolio.class);
    CacheUtils.createRegion("Ptfs1", Portfolio.class);
    QueryService qs = CacheUtils.getQueryService();
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/Portfolios");
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/Ptfs");
    Collection allIndexes = qs.getIndexes();
    if (allIndexes.size() != 2) {
      fail("QueryService.getIndexes() does not return correct indexes");
    }
  }

  @Test
  public void getIndexesShouldReturnEmptyCollectionWhenNoIndexesAreDefined() {
    CacheUtils.log("getIndexesShouldReturnEmptyCollectionWhenNoIndexesAreDefined");
    QueryService queryService = CacheUtils.getQueryService();
    Region replicatedRegion = CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE)
        .create("ReplicatedRegion");
    Region partitionedRegion = CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .create("PartitionedRegion");

    // Get All Indexes.
    assertThat(queryService.getIndexes()).isNotNull();
    assertThat(queryService.getIndexes().size()).isEqualTo(0);

    // Get All Indexes For Replicated Region.
    assertThat(queryService.getIndexes(replicatedRegion)).isNotNull();
    assertThat(queryService.getIndexes(replicatedRegion).isEmpty()).isTrue();

    // Get All Indexes For Partitioned Region.
    assertThat(queryService.getIndexes(partitionedRegion)).isNotNull();
    assertThat(queryService.getIndexes(partitionedRegion).isEmpty()).isTrue();
  }

  @Test
  public void getIndexesShouldReturnEmptyCollectionWhenNoIndexesOfTheSpecifiedTypeAreDefined() {
    CacheUtils
        .log("getIndexesShouldReturnEmptyCollectionWhenNoIndexesOfTheSpecifiedTypeAreDefined");
    IndexType[] indexTypes = IndexType.values();
    QueryService queryService = CacheUtils.getQueryService();
    Region replicatedRegion = CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE)
        .create("ReplicatedRegion");
    Region partitionedRegion = CacheUtils.getCache().createRegionFactory(RegionShortcut.PARTITION)
        .create("PartitionedRegion");

    // Get All Indexes.
    assertThat(queryService.getIndexes()).isNotNull();
    assertThat(queryService.getIndexes().size()).isEqualTo(0);

    // Get All Indexes For Replicated Region.
    Arrays.stream(indexTypes).forEach(indexType -> {
      assertThat(queryService.getIndexes(replicatedRegion, indexType)).isNotNull();
      assertThat(queryService.getIndexes(replicatedRegion, indexType).isEmpty()).isTrue();
    });

    // Get All Indexes For Partitioned Region.
    Arrays.stream(indexTypes).forEach(indexType -> {
      assertThat(queryService.getIndexes(partitionedRegion, indexType)).isNotNull();
      assertThat(queryService.getIndexes(partitionedRegion, indexType).isEmpty()).isTrue();
    });
  }
}
