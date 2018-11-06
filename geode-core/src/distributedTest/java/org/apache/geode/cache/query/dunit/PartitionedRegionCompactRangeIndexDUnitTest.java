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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.util.test.TestUtil;

@Category({OQLQueryTest.class})
public class PartitionedRegionCompactRangeIndexDUnitTest implements Serializable {
  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  protected MemberVM locator;

  Properties props;

  MemberVM server1;
  MemberVM server2;
  MemberVM server3;

  private Properties getSystemProperties(String cacheXML) {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    props.setProperty(CACHE_XML_FILE, TestUtil.getResourcePath(getClass(), cacheXML));
    return props;
  }

  @Before
  public void before() throws Exception {
    this.locator = this.clusterStartupRule.startLocatorVM(0);
    props = getSystemProperties("PersistentPartitionWithIndex.xml");

    server1 = this.clusterStartupRule.startServerVM(1, props, this.locator.getPort());
    server2 = this.clusterStartupRule.startServerVM(2, props, this.locator.getPort());
    server3 = this.clusterStartupRule.startServerVM(3, props, this.locator.getPort());

    // Adding due to known race condition for creation of partitioned indexes via cache.xml
    IgnoredException.addIgnoredException("IndexNameConflictException");
  }

  @Test
  public void testGIIUpdateWithIndexDoesNotDuplicateEntryInIndexWhenAlreadyRecoveredFromPersistence()
      throws Exception {
    String regionName = "persistentTestRegion"; // this region is created via cache.xml
    String idQuery = "select * from /" + regionName + " p where p.ID = 1";
    int idQueryExpectedSize = 1;
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(i)));

    server3.invoke(() -> populateRegion(regionName, entries));

    server2.invoke(verifyQueryResultsSize(idQuery, idQueryExpectedSize));
    clusterStartupRule.stop(2, false);

    // update entries
    server3.invoke(() -> populateRegion(regionName, entries));
    clusterStartupRule.stop(1, false);
    server1 = this.clusterStartupRule.startServerVM(1, props, this.locator.getPort());
    server2 = this.clusterStartupRule.startServerVM(2, props, this.locator.getPort());

    server3.invoke(verifyQueryResultsSize(idQuery, idQueryExpectedSize));
    server2.invoke(verifyQueryResultsSize(idQuery, idQueryExpectedSize));
    server1.invoke(verifyQueryResultsSize(idQuery, idQueryExpectedSize));
  }

  @Test
  public void giiWithPersistenceAndStaleDataDueToUpdatesShouldCorrectlyPopulateIndexes()
      throws Exception {
    String regionName = "persistentTestRegionWithEntrySetIndex"; // this region is created via
                                                                 // cache.xml
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(10000 + i)));
    server3.invoke(() -> populateRegion(regionName, entries));

    clusterStartupRule.stop(2, false);
    // update entries
    IntStream.range(0, numEntries).forEach(i -> {
      entries.put("key-" + i, new Portfolio(i));
    });
    server3.invoke(() -> populateRegion(regionName, entries));
    clusterStartupRule.stop(1, false);
    clusterStartupRule.stop(3, false);

    Thread t3 =
        new Thread(() -> this.clusterStartupRule.startServerVM(3, props, this.locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> this.clusterStartupRule.startServerVM(1, props, this.locator.getPort()));
    t1.start();
    this.clusterStartupRule.startServerVM(2, props, this.locator.getPort());
    t3.join();
    t1.join();

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server3.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
    server2.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
    server1.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
  }


  @Test
  public void giiWithPersistenceAndStaleDataDueToSameUpdatesShouldCorrectlyPopulateIndexes()
      throws Exception {
    String regionName = "persistentTestRegionWithEntrySetIndex"; // this region is created via
    // cache.xml
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(i)));
    server3.invoke(() -> populateRegion(regionName, entries));

    clusterStartupRule.stop(2, false);
    // update entries
    IntStream.range(0, numEntries).forEach(i -> {
      entries.put("key-" + i, new Portfolio(i));
    });
    server3.invoke(() -> populateRegion(regionName, entries));
    clusterStartupRule.stop(1, false);
    clusterStartupRule.stop(3, false);

    Thread t3 =
        new Thread(() -> this.clusterStartupRule.startServerVM(3, props, this.locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> this.clusterStartupRule.startServerVM(1, props, this.locator.getPort()));
    t1.start();
    this.clusterStartupRule.startServerVM(2, props, this.locator.getPort());
    t3.join();
    t1.join();

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server3.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
    server2.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
    server1.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
  }


  @Test
  public void giiWithPersistenceAndStaleDataDueToUpdatesShouldCorrectlyPopulateIndexesWithEntrySet()
      throws Exception {
    String regionName = "persistentTestRegionWithEntrySetIndex"; // this region is created via
                                                                 // cache.xml
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(10000 + i)));
    server3.invoke(() -> populateRegion(regionName, entries));

    clusterStartupRule.stop(2, false);

    // update entries
    IntStream.range(0, numEntries).forEach(i -> {
      entries.put("key-" + i, new Portfolio(i));
    });
    server3.invoke(() -> populateRegion(regionName, entries));
    clusterStartupRule.stop(1, false);
    clusterStartupRule.stop(3, false);

    Thread t3 =
        new Thread(() -> this.clusterStartupRule.startServerVM(3, props, this.locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> this.clusterStartupRule.startServerVM(1, props, this.locator.getPort()));
    t1.start();
    this.clusterStartupRule.startServerVM(2, props, this.locator.getPort());
    t3.join();
    t1.join();

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server3.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + ".entrySet where value.ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
    server2.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + ".entrySet where value.ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
    server1.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + ".entrySet where value.ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
  }

  @Test
  public void giiWithPersistenceAndStaleDataDueToDeletesShouldProvideCorrectResultsWithEntrySet()
      throws Exception {
    String regionName = "persistentTestRegionWithEntrySetIndex"; // this region is created via
                                                                 // cache.xml
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(i)));
    server3.invoke(() -> populateRegion(regionName, entries));

    clusterStartupRule.stop(2, false);

    server3.invoke(() -> destroyFromRegion(regionName, entries.keySet()));
    clusterStartupRule.stop(1, false);
    clusterStartupRule.stop(3, false);

    Thread t3 =
        new Thread(() -> this.clusterStartupRule.startServerVM(3, props, this.locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> this.clusterStartupRule.startServerVM(1, props, this.locator.getPort()));
    t1.start();
    this.clusterStartupRule.startServerVM(2, props, this.locator.getPort());
    t3.join();
    t1.join();

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server3.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + ".entrySet where value.ID = ",
          () -> IntStream.range(0, numEntries), 8, 0);
    });
    server2.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + ".entrySet where value.ID = ",
          () -> IntStream.range(0, numEntries), 8, 0);
    });
    server1.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + ".entrySet where value.ID = ",
          () -> IntStream.range(0, numEntries), 8, 0);
    });
  }

  @Test
  public void giiWithPersistenceAndStaleDataDueToDeletesShouldHaveEmptyIndexesWithEntrySet()
      throws Exception {
    String regionName = "persistentTestRegionWithEntrySetIndex"; // this region is created via
                                                                 // cache.xml
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(i)));
    server3.invoke(() -> populateRegion(regionName, entries));

    clusterStartupRule.stop(2, false);

    server3.invoke(() -> destroyFromRegion(regionName, entries.keySet()));
    clusterStartupRule.stop(1, false);
    clusterStartupRule.stop(3, false);

    Thread t3 =
        new Thread(() -> this.clusterStartupRule.startServerVM(3, props, this.locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> this.clusterStartupRule.startServerVM(1, props, this.locator.getPort()));
    t1.start();
    this.clusterStartupRule.startServerVM(2, props, this.locator.getPort());
    t3.join();
    t1.join();

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server3.invoke(() -> {
      verifyIndexKeysAreEmpty();
    });
    server2.invoke(() -> {
      verifyIndexKeysAreEmpty();
    });
    server1.invoke(() -> {
      verifyIndexKeysAreEmpty();
    });
  }

  @Test
  public void giiWithPersistenceAndStaleDataDueToDeletesShouldProvideCorrectResultsWithIndexes()
      throws Exception {
    String regionName = "persistentTestRegion"; // this region is created via cache.xml
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(i)));
    server3.invoke(() -> populateRegion(regionName, entries));

    clusterStartupRule.stop(2, false);
    server3.invoke(() -> destroyFromRegion(regionName, entries.keySet()));
    clusterStartupRule.stop(1, false);
    clusterStartupRule.stop(3, false);

    Thread t3 =
        new Thread(() -> this.clusterStartupRule.startServerVM(3, props, this.locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> this.clusterStartupRule.startServerVM(1, props, this.locator.getPort()));
    t1.start();
    this.clusterStartupRule.startServerVM(2, props, this.locator.getPort());
    t3.join();
    t1.join();

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server3.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 0);
    });
    server2.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 0);
    });
    server1.invoke(() -> {
      verifyAllEntries("select key, value from /" + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 0);
    });
  }

  @Test
  public void giiWithPersistenceAndStaleDataDueToDeletesShouldHaveEmptyIndexes()
      throws Exception {
    String regionName = "persistentTestRegion"; // this region is created via cache.xml
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(i)));
    server3.invoke(() -> populateRegion(regionName, entries));

    clusterStartupRule.stop(2, false);

    server3.invoke(() -> destroyFromRegion(regionName, entries.keySet()));
    clusterStartupRule.stop(1, false);
    clusterStartupRule.stop(3, false);

    Thread t3 =
        new Thread(() -> this.clusterStartupRule.startServerVM(3, props, this.locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> this.clusterStartupRule.startServerVM(1, props, this.locator.getPort()));
    t1.start();
    this.clusterStartupRule.startServerVM(2, props, this.locator.getPort());
    t3.join();
    t1.join();

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server3.invoke(() -> {
      verifyIndexKeysAreEmpty();
    });
    server2.invoke(() -> {
      verifyIndexKeysAreEmpty();
    });
    server1.invoke(() -> {
      verifyIndexKeysAreEmpty();
    });
  }

  private void verifyAllEntries(String query, Supplier<IntStream> idsSupplier, int numTimes,
      int expectedSize) throws Exception {
    for (int j = 0; j < numTimes; j++) {
      idsSupplier.get().forEach(i -> {
        try {
          verifyQueryResultsSize(query + i, expectedSize).run();
        } catch (Exception e) {
          fail();
        }
      });
    }
  }

  private SerializableRunnable verifyQueryResultsSize(String query, int expectedSize) {
    return new SerializableRunnable() {
      public void run() {
        try {
          QueryService qs = ClusterStartupRule.getCache().getQueryService();
          Query q = qs.newQuery(query);
          SelectResults sr = (SelectResults) q.execute();
          assertEquals(expectedSize, sr.size());
        } catch (Exception e) {
          e.printStackTrace();
          fail("Exception occurred when executing verifyQueryResultsSize for query:" + query);
        }
      }
    };
  }

  private void verifyIndexKeysAreEmpty() {
    QueryService qs = ClusterStartupRule.getCache().getQueryService();
    qs.getIndexes().forEach(index -> assertEquals(0, index.getStatistics().getNumberOfKeys()));
  }

  private void populateRegion(String regionName, Map<String, Portfolio> entries) {
    Region r = ClusterStartupRule.getCache().getRegion("/" + regionName);
    entries.entrySet().forEach(e -> {
      r.put(e.getKey(), e.getValue());
    });
  }

  private void destroyFromRegion(String regionName, Collection keys) {
    Region r = ClusterStartupRule.getCache().getRegion("/" + regionName);
    keys.forEach(i -> r.remove(i));
  }
}
