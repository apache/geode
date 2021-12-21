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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category(OQLQueryTest.class)
@RunWith(value = Parameterized.class)
public class PersistentRegionCompactRangeIndexDUnitTest implements Serializable {
  @Parameterized.Parameters(name = "{index}: {0}")
  public static Collection<String> cacheXmlFiles() {
    return Arrays.asList("PartitionedPersistentRegionWithIndex.xml",
        "ReplicatePersistentRegionWithIndex.xml");
  }

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  private MemberVM locator;

  private final String cacheXml;

  private Properties props;

  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;


  public PersistentRegionCompactRangeIndexDUnitTest(String cacheXml) {
    this.cacheXml = cacheXml;
  }

  private Properties getSystemProperties(String cacheXML) {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    props.setProperty(CACHE_XML_FILE,
        createTempFileFromResource(getClass(), cacheXML).getAbsolutePath());
    return props;
  }

  @Before
  public void before() {
    locator = clusterStartupRule.startLocatorVM(0);
    props = getSystemProperties(cacheXml);

    server1 = clusterStartupRule.startServerVM(1, props, locator.getPort());
    server2 = clusterStartupRule.startServerVM(2, props, locator.getPort());
    server3 = clusterStartupRule.startServerVM(3, props, locator.getPort());

    // Adding due to known race condition for creation of partitioned indexes via cache.xml
    addIgnoredException(IndexNameConflictException.class);
  }

  @Test
  public void testGIIUpdateWithIndexDoesNotDuplicateEntryInIndexWhenAlreadyRecoveredFromPersistence() {
    // this region is created via cache.xml
    String regionName = "testRegion";
    String idQuery = "select * from " + SEPARATOR + regionName + " p where p.ID = 1";
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
    server1 = clusterStartupRule.startServerVM(1, props, locator.getPort());
    server2 = clusterStartupRule.startServerVM(2, props, locator.getPort());

    server3.invoke(verifyQueryResultsSize(idQuery, idQueryExpectedSize));
    server2.invoke(verifyQueryResultsSize(idQuery, idQueryExpectedSize));
    server1.invoke(verifyQueryResultsSize(idQuery, idQueryExpectedSize));
  }

  @Test
  public void giiWithPersistenceAndStaleDataDueToUpdatesShouldCorrectlyPopulateIndexes()
      throws Exception {
    // this region is created via cache.xml
    String regionName = "testRegionWithEntrySetIndex";
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
        new Thread(() -> clusterStartupRule.startServerVM(3, props, locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> clusterStartupRule.startServerVM(1, props, locator.getPort()));
    t1.start();
    clusterStartupRule.startServerVM(2, props, locator.getPort());
    t3.join();
    t1.join();

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server3.invoke(() -> {
      verifyAllEntries("select key, value from " + SEPARATOR + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
    server2.invoke(() -> {
      verifyAllEntries("select key, value from " + SEPARATOR + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
    server1.invoke(() -> {
      verifyAllEntries("select key, value from " + SEPARATOR + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
  }

  @Test
  public void giiWhereNewAndOldValueIsTombstoneShouldNotThrowNPE()
      throws Exception {
    // This test requires the oldKeyValuePair to be instantiated, to do so, we need the gii thread
    // to gii some updates or removes on existing (non-tombstone) values before processing the
    // scenario of tombstone to tombstone
    String regionName = "testRegion";
    int numEntries = 10;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(i)));
    Set halfEntries = new HashSet();
    IntStream.range(0, numEntries).forEach(i -> {
      if (i % 2 == 0) {
        halfEntries.add("key-" + i);
      }
    });

    server1.invoke(() -> populateRegion(regionName, entries));
    server1.invoke(() -> destroyFromRegion(regionName, halfEntries));

    clusterStartupRule.stop(2, false);


    // update entries
    server1.invoke(() -> populateRegion(regionName, entries));
    server1.invoke(() -> destroyFromRegion(regionName, halfEntries));
    clusterStartupRule.startServerVM(2, props, locator.getPort());

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server1.invoke(() -> {
      verifyAllEntries("select key, value from " + SEPARATOR + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries).filter(i -> i % 2 != 0), 8, 1);
    });
    server2.invoke(() -> {
      verifyAllEntries("select key, value from " + SEPARATOR + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries).filter(i -> i % 2 != 0), 8, 1);
    });
    server1.invoke(() -> {
      verifyAllEntries("select key, value from " + SEPARATOR + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries).filter(i -> i % 2 != 0), 8, 1);
    });
  }

  @Test
  public void giiWithPersistenceAndStaleDataDueToSameUpdatesShouldCorrectlyPopulateIndexes()
      throws Exception {
    // this region is created via cache.xml
    String regionName = "testRegionWithEntrySetIndex";
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
        new Thread(() -> clusterStartupRule.startServerVM(3, props, locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> clusterStartupRule.startServerVM(1, props, locator.getPort()));
    t1.start();
    clusterStartupRule.startServerVM(2, props, locator.getPort());
    t3.join();
    t1.join();

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server3.invoke(() -> {
      verifyAllEntries("select key, value from " + SEPARATOR + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
    server2.invoke(() -> {
      verifyAllEntries("select key, value from " + SEPARATOR + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
    server1.invoke(() -> {
      verifyAllEntries("select key, value from " + SEPARATOR + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
  }

  @Test
  public void giiWithPersistenceAndStaleDataDueToUpdatesShouldCorrectlyPopulateIndexesWithEntrySet()
      throws Exception {
    // this region is created via cache.xml
    String regionName = "testRegionWithEntrySetIndex";
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
        new Thread(() -> clusterStartupRule.startServerVM(3, props, locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> clusterStartupRule.startServerVM(1, props, locator.getPort()));
    t1.start();
    clusterStartupRule.startServerVM(2, props, locator.getPort());
    t3.join();
    t1.join();

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server3.invoke(() -> {
      verifyAllEntries(
          "select key, value from " + SEPARATOR + regionName + ".entrySet where value.ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
    server2.invoke(() -> {
      verifyAllEntries(
          "select key, value from " + SEPARATOR + regionName + ".entrySet where value.ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
    server1.invoke(() -> {
      verifyAllEntries(
          "select key, value from " + SEPARATOR + regionName + ".entrySet where value.ID = ",
          () -> IntStream.range(0, numEntries), 8, 1);
    });
  }

  @Test
  public void giiWithPersistenceAndStaleDataDueToDeletesShouldProvideCorrectResultsWithEntrySet()
      throws Exception {
    // this region is created via cache.xml
    String regionName = "testRegionWithEntrySetIndex";
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(i)));
    server3.invoke(() -> populateRegion(regionName, entries));

    clusterStartupRule.stop(2, false);

    server3.invoke(() -> destroyFromRegion(regionName, entries.keySet()));
    clusterStartupRule.stop(1, false);
    clusterStartupRule.stop(3, false);

    Thread t3 =
        new Thread(() -> clusterStartupRule.startServerVM(3, props, locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> clusterStartupRule.startServerVM(1, props, locator.getPort()));
    t1.start();
    clusterStartupRule.startServerVM(2, props, locator.getPort());
    t3.join();
    t1.join();

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server3.invoke(() -> {
      verifyAllEntries(
          "select key, value from " + SEPARATOR + regionName + ".entrySet where value.ID = ",
          () -> IntStream.range(0, numEntries), 8, 0);
    });
    server2.invoke(() -> {
      verifyAllEntries(
          "select key, value from " + SEPARATOR + regionName + ".entrySet where value.ID = ",
          () -> IntStream.range(0, numEntries), 8, 0);
    });
    server1.invoke(() -> {
      verifyAllEntries(
          "select key, value from " + SEPARATOR + regionName + ".entrySet where value.ID = ",
          () -> IntStream.range(0, numEntries), 8, 0);
    });
  }

  @Test
  public void giiWithPersistenceAndStaleDataDueToDeletesShouldHaveEmptyIndexesWithEntrySet()
      throws Exception {
    // this region is created via cache.xml
    String regionName = "testRegionWithEntrySetIndex";
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(i)));
    server3.invoke(() -> populateRegion(regionName, entries));

    clusterStartupRule.stop(2, false);

    server3.invoke(() -> destroyFromRegion(regionName, entries.keySet()));
    clusterStartupRule.stop(1, false);
    clusterStartupRule.stop(3, false);

    Thread t3 =
        new Thread(() -> clusterStartupRule.startServerVM(3, props, locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> clusterStartupRule.startServerVM(1, props, locator.getPort()));
    t1.start();
    clusterStartupRule.startServerVM(2, props, locator.getPort());
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
    // this region is created via cache.xml
    String regionName = "testRegion";
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(i)));
    server3.invoke(() -> populateRegion(regionName, entries));

    clusterStartupRule.stop(2, false);
    server3.invoke(() -> destroyFromRegion(regionName, entries.keySet()));
    clusterStartupRule.stop(1, false);
    clusterStartupRule.stop(3, false);

    Thread t3 =
        new Thread(() -> clusterStartupRule.startServerVM(3, props, locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> clusterStartupRule.startServerVM(1, props, locator.getPort()));
    t1.start();
    clusterStartupRule.startServerVM(2, props, locator.getPort());
    t3.join();
    t1.join();

    // invoke the query enough times to hopefully randomize bucket to server targeting enough to
    // target both secondary/primary servers
    server3.invoke(() -> {
      verifyAllEntries("select key, value from " + SEPARATOR + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 0);
    });
    server2.invoke(() -> {
      verifyAllEntries("select key, value from " + SEPARATOR + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 0);
    });
    server1.invoke(() -> {
      verifyAllEntries("select key, value from " + SEPARATOR + regionName + " where ID = ",
          () -> IntStream.range(0, numEntries), 8, 0);
    });
  }

  @Test
  public void giiWithPersistenceAndStaleDataDueToDeletesShouldHaveEmptyIndexes()
      throws Exception {
    // this region is created via cache.xml
    String regionName = "testRegion";
    int numEntries = 100;
    Map<String, Portfolio> entries = new HashMap<>();
    IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, new Portfolio(i)));
    server3.invoke(() -> populateRegion(regionName, entries));

    clusterStartupRule.stop(2, false);

    server3.invoke(() -> destroyFromRegion(regionName, entries.keySet()));
    clusterStartupRule.stop(1, false);
    clusterStartupRule.stop(3, false);

    Thread t3 =
        new Thread(() -> clusterStartupRule.startServerVM(3, props, locator.getPort()));
    t3.start();
    Thread t1 =
        new Thread(() -> clusterStartupRule.startServerVM(1, props, locator.getPort()));
    t1.start();
    clusterStartupRule.startServerVM(2, props, locator.getPort());
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
      int expectedSize) {
    for (int j = 0; j < numTimes; j++) {
      idsSupplier.get().forEach(i -> {
        try {
          verifyQueryResultsSize(query + i, expectedSize).run();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  private SerializableRunnable verifyQueryResultsSize(String query, int expectedSize) {
    return new SerializableRunnable() {
      @Override
      public void run() {
        try {
          QueryService qs = ClusterStartupRule.getCache().getQueryService();
          Query q = qs.newQuery(query);
          SelectResults sr = (SelectResults) q.execute();
          assertEquals(expectedSize, sr.size());
        } catch (Exception e) {
          throw new RuntimeException(
              "Exception occurred when executing verifyQueryResultsSize for query:" + query, e);
        }
      }
    };
  }

  private void verifyIndexKeysAreEmpty() {
    QueryService qs = ClusterStartupRule.getCache().getQueryService();
    qs.getIndexes().forEach(index -> assertEquals(0, index.getStatistics().getNumberOfKeys()));
  }

  private void populateRegion(String regionName, Map<String, Portfolio> entries) {
    Region r = ClusterStartupRule.getCache().getRegion(SEPARATOR + regionName);
    entries.entrySet().forEach(e -> {
      r.put(e.getKey(), e.getValue());
    });
  }

  private void destroyFromRegion(String regionName, Collection keys) {
    Region r = ClusterStartupRule.getCache().getRegion(SEPARATOR + regionName);
    keys.forEach(i -> r.remove(i));
  }
}
