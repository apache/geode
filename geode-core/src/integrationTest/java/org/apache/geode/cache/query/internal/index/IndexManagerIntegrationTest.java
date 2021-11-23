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
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.IntStream;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category(OQLIndexTest.class)
@RunWith(GeodeParamsRunner.class)
public class IndexManagerIntegrationTest {
  private File logFile;
  private final int entries = 100;
  private InternalCache internalCache;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule();

  @Before
  public void setUp() throws IOException, ClassNotFoundException {
    TestQueryObject.throwException = false;

    logFile = temporaryFolder.newFile(testName.getMethodName() + ".log");
    serverStarterRule.withProperty("log-file", logFile.getAbsolutePath())
        .withProperty("log-level", "debug").startServer();
    internalCache = serverStarterRule.getCache();
  }

  @SuppressWarnings("unused")
  private Object[] getRegionAndIndexMaintenanceTypes() {
    return new Object[] {
        new Object[] {"LOCAL", "true"},
        new Object[] {"LOCAL", "false"},

        new Object[] {"REPLICATE", "true"},
        new Object[] {"REPLICATE", "false"},

        new Object[] {"PARTITION", "true"},
        new Object[] {"PARTITION", "false"},
    };
  }

  private void waitForIndexUpdaterTask(boolean synchronousMaintenance, Region region) {
    if (!synchronousMaintenance) {
      InternalRegion internalRegion = (InternalRegion) region;
      await().untilAsserted(
          () -> assertThat(internalRegion.getIndexManager().getUpdaterThread().isDone()).isTrue());
    }
  }

  private Region<Integer, TestQueryObject> createAndPopulateRegionWithIndex(
      RegionShortcut regionShortcut, String regionName, String indexName,
      boolean synchronousMaintenance)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    Region<Integer, TestQueryObject> region = internalCache
        .<Integer, TestQueryObject>createRegionFactory(regionShortcut)
        .setIndexMaintenanceSynchronous(synchronousMaintenance)
        .create(regionName);

    QueryService queryService = internalCache.getQueryService();
    queryService.createIndex(indexName, "id", SEPARATOR + regionName);
    IntStream.range(1, entries + 1).forEach(i -> region.put(i, new TestQueryObject(i)));
    waitForIndexUpdaterTask(synchronousMaintenance, region);

    Index index = queryService.getIndex(region, indexName);
    assertThat(index.isValid()).isTrue();

    return region;
  }

  @Test
  @Parameters(method = "getRegionAndIndexMaintenanceTypes")
  @TestCaseName("[{index}] {method}(RegionType:{0};IndexSynchronousMaintenance:{1})")
  public void indexShouldBeMarkedAsInvalidWhenAddMappingOperationFailsAfterEntryAddition(
      RegionShortcut regionShortcut, boolean synchronousMaintenance) throws Exception {
    int newKey = entries + 2;
    String regionName = testName.getMethodName();
    String indexName = testName.getMethodName() + "_index";
    Region<Integer, TestQueryObject> region = createAndPopulateRegionWithIndex(regionShortcut,
        regionName, indexName, synchronousMaintenance);

    // Create entry, add mapping will throw an exception when invoking 'getId()'.
    TestQueryObject.throwException = true;
    assertThat(region.containsKey(newKey)).isFalse();
    TestQueryObject testQueryObject = new TestQueryObject(0);
    assertThatCode(() -> region.create(newKey, testQueryObject)).doesNotThrowAnyException();
    waitForIndexUpdaterTask(synchronousMaintenance, region);

    // Entry created, index marked as invalid, and error logged.
    assertThat(region.containsKey(newKey)).isTrue();
    assertThat(region.get(newKey)).isEqualTo(testQueryObject);
    Index indexInvalid = internalCache.getQueryService().getIndex(region, indexName);
    assertThat(indexInvalid.isValid()).isFalse();
    LogFileAssert.assertThat(logFile)
        .contains(String.format(
            "Updating the Index %s failed. The index is corrupted and marked as invalid.",
            indexName));
    LogFileAssert.assertThat(logFile).contains("Corrupted key is " + newKey);
  }

  @Test
  @Parameters(method = "getRegionAndIndexMaintenanceTypes")
  @TestCaseName("[{index}] {method}(RegionType:{0};IndexSynchronousMaintenance:{1})")
  public void indexShouldBeMarkedAsInvalidWhenAddMappingOperationFailsAfterEntryModification(
      RegionShortcut regionShortcut, boolean synchronousMaintenance) throws Exception {
    int existingKey = entries / 2;
    String regionName = testName.getMethodName();
    String indexName = testName.getMethodName() + "_index";
    Region<Integer, TestQueryObject> region = createAndPopulateRegionWithIndex(regionShortcut,
        regionName, indexName, synchronousMaintenance);

    // Update entry, add mapping will throw an exception when invoking 'getId()'.
    TestQueryObject.throwException = true;
    TestQueryObject testQueryObject = new TestQueryObject(0);
    assertThatCode(() -> region.put(existingKey, testQueryObject)).doesNotThrowAnyException();
    waitForIndexUpdaterTask(synchronousMaintenance, region);

    // Entry updated, index marked as invalid, and error logged.
    assertThat(region.get(existingKey)).isEqualTo(testQueryObject);
    Index indexInvalid = internalCache.getQueryService().getIndex(region, indexName);
    assertThat(indexInvalid.isValid()).isFalse();
    LogFileAssert.assertThat(logFile)
        .contains(String.format(
            "Updating the Index %s failed. The index is corrupted and marked as invalid.",
            indexName));
    LogFileAssert.assertThat(logFile).contains("Corrupted key is " + existingKey);
  }

  @Test
  @Parameters(method = "getRegionAndIndexMaintenanceTypes")
  @TestCaseName("[{index}] {method}(RegionType:{0};IndexSynchronousMaintenance:{1})")
  public void indexShouldBeMarkedAsInvalidWhenAddMappingOperationFailsAfterEntryDeletion(
      RegionShortcut regionShortcut, boolean synchronousMaintenance) throws Exception {
    int existingKey = entries / 3;
    String regionName = testName.getMethodName();
    String indexName = testName.getMethodName() + "_index";
    Region<Integer, TestQueryObject> region = createAndPopulateRegionWithIndex(regionShortcut,
        regionName, indexName, synchronousMaintenance);

    // Remove entry, remove mapping will throw an exception when invoking 'getId()'.
    TestQueryObject.throwException = true;
    assertThat(region.containsKey(existingKey)).isTrue();

    // Make sure we get the exception for asynchronous indexes.
    if (!synchronousMaintenance) {
      // RangeIndex for asynchronous maintenance, hack internal structures to throw exception.
      Index index = internalCache.getQueryService().getIndex(region, indexName);

      if (!PARTITION.equals(regionShortcut)) {
        ((RangeIndex) index).valueToEntriesMap.clear();
      } else {
        @SuppressWarnings("unchecked")
        List<RangeIndex> bucketRangeIndexList = ((PartitionedIndex) index).getBucketIndexes();
        bucketRangeIndexList.forEach(rangeIndex -> rangeIndex.valueToEntriesMap.clear());
      }
    }

    assertThatCode(() -> region.destroy(existingKey)).doesNotThrowAnyException();
    waitForIndexUpdaterTask(synchronousMaintenance, region);

    // Entry deleted, index marked as invalid, and error logged.
    assertThat(region.get(existingKey)).isNull();
    Index indexInvalid = internalCache.getQueryService().getIndex(region, indexName);
    assertThat(indexInvalid.isValid()).isFalse();
    LogFileAssert.assertThat(logFile)
        .contains(String.format(
            "Updating the Index %s failed. The index is corrupted and marked as invalid.",
            indexName));
    LogFileAssert.assertThat(logFile).contains("Corrupted key is " + existingKey);
  }

  private static class TestQueryObject implements Serializable {
    private final int id;
    static transient boolean throwException = false;

    public int getId() {
      if (throwException) {
        throw new RuntimeException("Mock Exception");
      } else {
        return id;
      }
    }

    TestQueryObject(int id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestQueryObject that = (TestQueryObject) o;
      return id == that.id;
    }

    @Override
    public int hashCode() {
      return id;
    }
  }
}
