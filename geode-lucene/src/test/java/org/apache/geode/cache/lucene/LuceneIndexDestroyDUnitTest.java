/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.internal.LuceneIndexForPartitionedRegion;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.test.TestObject;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.cache.snapshot.SnapshotOptions;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.awaitility.Awaitility;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.apache.geode.internal.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneIndexDestroyDUnitTest extends LuceneDUnitTest {

  private volatile boolean STOP_PUTS = false;

  protected VM accessor;

  @Override
  public void postSetUp() throws Exception {
    super.postSetUp();
    accessor = Host.getHost(0).getVM(3);
  }

  private final Object[] parametersForIndexDestroys() {
    String[] destroyDataRegionParameters = {"true", "false"};
    RegionTestableType[] regionTestTypes = getListOfRegionTestTypes();
    return parameterCombiner(destroyDataRegionParameters, regionTestTypes);
  }

  @Test
  @Parameters(method = "parametersForIndexDestroys")
  public void verifyDestroySingleIndex(boolean destroyDataRegion, RegionTestableType regionType) {
    // Create index and region
    dataStore1.invoke(() -> initDataStore(createIndex(), regionType));
    dataStore2.invoke(() -> initDataStore(createIndex(), regionType));

    // Verify index created
    dataStore1.invoke(() -> verifyIndexCreated());
    dataStore2.invoke(() -> verifyIndexCreated());

    // Attempt to destroy data region (should fail)
    if (destroyDataRegion) {
      dataStore1.invoke(() -> destroyDataRegion(false));
    }

    // Destroy index (only needs to be done on one member)
    dataStore1.invoke(() -> destroyIndex());

    // Verify index destroyed
    dataStore1.invoke(() -> verifyIndexDestroyed());
    dataStore2.invoke(() -> verifyIndexDestroyed());

    // Attempt to destroy data region (should succeed)
    if (destroyDataRegion) {
      dataStore1.invoke(() -> destroyDataRegion(true));
    }
  }

  @Test
  @Parameters(method = "parametersForIndexDestroys")
  public void verifyDestroyAllIndexes(boolean destroyDataRegion, RegionTestableType regionType) {
    // Create indexes and region
    dataStore1.invoke(() -> initDataStore(createIndexes(), regionType));
    dataStore2.invoke(() -> initDataStore(createIndexes(), regionType));

    // Verify indexes created
    dataStore1.invoke(() -> verifyIndexesCreated());
    dataStore2.invoke(() -> verifyIndexesCreated());

    // Attempt to destroy data region (should fail)
    if (destroyDataRegion) {
      dataStore1.invoke(() -> destroyDataRegion(false));
    }

    // Destroy indexes (only needs to be done on one member)
    dataStore1.invoke(() -> destroyIndexes());

    // Verify indexes destroyed
    dataStore1.invoke(() -> verifyIndexesDestroyed());
    dataStore2.invoke(() -> verifyIndexesDestroyed());

    // Attempt to destroy data region (should succeed)
    if (destroyDataRegion) {
      dataStore1.invoke(() -> destroyDataRegion(true));
    }
  }

  @Ignore
  // Destroying an index while puts are occurring currently fails with a
  // GatewaySenderConfigurationException.
  @Parameters(method = "getListOfServerRegionTestTypes")
  public void verifyDestroySingleIndexWhileDoingPuts(RegionTestableType regionType)
      throws Exception {
    // Create index and region
    dataStore1.invoke(() -> initDataStore(createIndex(), regionType));
    dataStore2.invoke(() -> initDataStore(createIndex(), regionType));

    // Verify index created
    dataStore1.invoke(() -> verifyIndexCreated());
    dataStore2.invoke(() -> verifyIndexCreated());

    // Start puts
    AsyncInvocation putter = dataStore1.invokeAsync(() -> doPutsUntilStopped());

    // Wait until puts have started
    dataStore1.invoke(() -> waitUntilPutsHaveStarted());

    // Destroy index (only needs to be done on one member)
    dataStore1.invoke(() -> destroyIndex());

    // Verify index destroyed
    dataStore1.invoke(() -> verifyIndexDestroyed());
    dataStore2.invoke(() -> verifyIndexDestroyed());

    // End puts
    dataStore1.invoke(() -> stopPuts());
    putter.join();
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void verifyDestroyRecreateIndexSameName(RegionTestableType regionType) {
    // Create index and region
    SerializableRunnableIF createIndex = createIndex();
    dataStore1.invoke(() -> initDataStore(createIndex, regionType));
    dataStore2.invoke(() -> initDataStore(createIndex, regionType));
    accessor.invoke(() -> initAccessor(createIndex, regionType));

    // Verify index created
    dataStore1.invoke(() -> verifyIndexCreated());
    dataStore2.invoke(() -> verifyIndexCreated());
    accessor.invoke(() -> verifyIndexCreated());

    // Do puts to cause IndexRepositories to be created
    int numPuts = 10;
    accessor.invoke(() -> doPuts(numPuts));

    // Wait until queue is flushed
    dataStore1.invoke(() -> waitUntilFlushed(INDEX_NAME));
    dataStore2.invoke(() -> waitUntilFlushed(INDEX_NAME));

    // Execute query and verify results
    accessor.invoke(() -> executeQuery(INDEX_NAME, "field1Value", "field1", numPuts));

    // Export entries from region
    accessor.invoke(() -> exportData(regionType));

    // Destroy indexes (only needs to be done on one member)
    dataStore1.invoke(() -> destroyIndexes());

    // Verify indexes destroyed
    dataStore1.invoke(() -> verifyIndexesDestroyed());
    dataStore2.invoke(() -> verifyIndexesDestroyed());

    // Destroy data region
    dataStore1.invoke(() -> destroyDataRegion(true));

    // Recreate index and region
    dataStore1.invoke(() -> initDataStore(createIndex, regionType));
    dataStore2.invoke(() -> initDataStore(createIndex, regionType));
    accessor.invoke(() -> initAccessor(createIndex, regionType));

    // Import entries into region
    accessor.invoke(() -> importData(regionType, numPuts));

    // Wait until queue is flushed
    // This verifies there are no deadlocks
    dataStore1.invoke(() -> waitUntilFlushed(INDEX_NAME));
    dataStore2.invoke(() -> waitUntilFlushed(INDEX_NAME));

    // re-execute query and verify results
    accessor.invoke(() -> executeQuery(INDEX_NAME, "field1Value", "field1", numPuts));
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void verifyDestroyRecreateIndexDifferentName(RegionTestableType regionType) {
    // Create index and region
    SerializableRunnableIF createIndex = createIndex();
    dataStore1.invoke(() -> initDataStore(createIndex, regionType));
    dataStore2.invoke(() -> initDataStore(createIndex, regionType));
    accessor.invoke(() -> initAccessor(createIndex, regionType));

    // Verify index created
    dataStore1.invoke(() -> verifyIndexCreated());
    dataStore2.invoke(() -> verifyIndexCreated());
    accessor.invoke(() -> verifyIndexCreated());

    // Do puts to cause IndexRepositories to be created
    int numPuts = 10;
    accessor.invoke(() -> doPuts(numPuts));

    // Wait until queue is flushed
    dataStore1.invoke(() -> waitUntilFlushed(INDEX_NAME));
    dataStore2.invoke(() -> waitUntilFlushed(INDEX_NAME));

    // Execute query and verify results
    accessor.invoke(() -> executeQuery(INDEX_NAME, "field1Value", "field1", numPuts));

    // Export entries from region
    accessor.invoke(() -> exportData(regionType));

    // Destroy indexes (only needs to be done on one member)
    dataStore1.invoke(() -> destroyIndexes());

    // Verify indexes destroyed
    dataStore1.invoke(() -> verifyIndexesDestroyed());
    dataStore2.invoke(() -> verifyIndexesDestroyed());

    // Destroy data region
    dataStore1.invoke(() -> destroyDataRegion(true));

    // Recreate index and region
    String newIndexName = INDEX_NAME + "+_1";
    SerializableRunnableIF createIndexNewName = createIndex(newIndexName, "field1");
    dataStore1.invoke(() -> initDataStore(createIndexNewName, regionType));
    dataStore2.invoke(() -> initDataStore(createIndexNewName, regionType));
    accessor.invoke(() -> initAccessor(createIndexNewName, regionType));

    // Import entries into region
    accessor.invoke(() -> importData(regionType, numPuts));

    // Wait until queue is flushed
    // This verifies there are no deadlocks
    dataStore1.invoke(() -> waitUntilFlushed(newIndexName));
    dataStore2.invoke(() -> waitUntilFlushed(newIndexName));

    // re-execute query and verify results
    accessor.invoke(() -> executeQuery(newIndexName, "field1Value", "field1", numPuts));
  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void verifyDestroyRecreateDifferentIndex(RegionTestableType regionType) {
    SerializableRunnableIF createIndex = createIndex();
    dataStore1.invoke(() -> initDataStore(createIndex, regionType));
    dataStore2.invoke(() -> initDataStore(createIndex, regionType));
    accessor.invoke(() -> initAccessor(createIndex, regionType));

    // Verify index created
    dataStore1.invoke(() -> verifyIndexCreated());
    dataStore2.invoke(() -> verifyIndexCreated());
    accessor.invoke(() -> verifyIndexCreated());

    // Do puts to cause IndexRepositories to be created
    int numPuts = 10;
    accessor.invoke(() -> doPuts(numPuts));

    // Wait until queue is flushed
    dataStore1.invoke(() -> waitUntilFlushed(INDEX_NAME));
    dataStore2.invoke(() -> waitUntilFlushed(INDEX_NAME));

    // Execute query and verify results
    accessor.invoke(() -> executeQuery(INDEX_NAME, "field1Value", "field1", numPuts));

    // Export entries from region
    accessor.invoke(() -> exportData(regionType));

    // Destroy indexes (only needs to be done on one member)
    dataStore1.invoke(() -> destroyIndexes());

    // Verify indexes destroyed
    dataStore1.invoke(() -> verifyIndexesDestroyed());
    dataStore2.invoke(() -> verifyIndexesDestroyed());

    // Destroy data region
    dataStore1.invoke(() -> destroyDataRegion(true));

    // Create new index and region
    SerializableRunnableIF createNewIndex = createIndex(INDEX_NAME, "field2");
    dataStore1.invoke(() -> initDataStore(createNewIndex, regionType));
    dataStore2.invoke(() -> initDataStore(createNewIndex, regionType));
    accessor.invoke(() -> initAccessor(createNewIndex, regionType));

    // Import entries into region
    accessor.invoke(() -> importData(regionType, numPuts));

    // Wait until queue is flushed
    // This verifies there are no deadlocks
    dataStore1.invoke(() -> waitUntilFlushed(INDEX_NAME));
    dataStore2.invoke(() -> waitUntilFlushed(INDEX_NAME));

    // re-execute query and verify results
    accessor.invoke(() -> executeQuery(INDEX_NAME, "field2Value", "field2", numPuts));
  }

  private SerializableRunnableIF createIndex() {
    return createIndex(INDEX_NAME, "field1");
  }

  private SerializableRunnableIF createIndex(String indexName, String field) {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(indexName, REGION_NAME, field);
    };
  }

  private SerializableRunnableIF createIndexes() {
    return () -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      luceneService.createIndex(INDEX_NAME + "0", REGION_NAME, "text");
      luceneService.createIndex(INDEX_NAME + "1", REGION_NAME, "text");
    };
  }

  private void verifyIndexCreated() {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    assertNotNull(luceneService.getIndex(INDEX_NAME, REGION_NAME));
  }

  private void verifyIndexesCreated() {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    assertNotNull(luceneService.getIndex(INDEX_NAME + "0", REGION_NAME));
    assertNotNull(luceneService.getIndex(INDEX_NAME + "1", REGION_NAME));
  }

  private void waitUntilFlushed(String indexName) throws Exception {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    assertTrue(
        luceneService.waitUntilFlushed(indexName, REGION_NAME, 30000, TimeUnit.MILLISECONDS));
  }

  private void doPuts(int numPuts) throws Exception {
    Region region = getCache().getRegion(REGION_NAME);
    for (int i = 0; i < numPuts; i++) {
      region.put(i, new TestObject("field1Value", "field2Value"));
    }
  }

  private void doPutsUntilStopped() throws Exception {
    Region region = getCache().getRegion(REGION_NAME);
    int i = 0;
    while (!STOP_PUTS) {
      region.put(i++, new TestObject());
      // Thread.sleep(50);
    }
  }

  private void stopPuts() {
    STOP_PUTS = true;
  }

  private void waitUntilPutsHaveStarted() {
    Awaitility.waitAtMost(30, TimeUnit.SECONDS)
        .until(() -> getCache().getRegion(REGION_NAME).size() > 0);
  }

  private void executeQuery(String indexName, String queryString, String field,
      int expectedResultsSize) throws LuceneQueryException {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    LuceneQuery query =
        luceneService.createLuceneQueryFactory().create(indexName, REGION_NAME, queryString, field);
    Collection results = query.findValues();
    assertEquals(expectedResultsSize, results.size());
  }

  private void destroyDataRegion(boolean shouldSucceed) {
    Region region = getCache().getRegion(REGION_NAME);
    assertNotNull(region);
    try {
      region.destroyRegion();
      if (!shouldSucceed) {
        fail("should not have been able to destroy data region named " + region.getFullPath());
      }
    } catch (IllegalStateException e) {
      if (shouldSucceed) {
        fail(e);
      }
    }
  }

  private void destroyIndex() {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    luceneService.destroyIndex(INDEX_NAME, REGION_NAME);
  }

  private void destroyIndexes() {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());
    luceneService.destroyIndexes(REGION_NAME);
  }

  private void verifyIndexDestroyed() {
    verifyIndexDestroyed(INDEX_NAME);
  }

  private void verifyIndexesDestroyed() {
    verifyIndexDestroyed(INDEX_NAME + "0");
    verifyIndexDestroyed(INDEX_NAME + "1");
  }

  private void verifyIndexDestroyed(String indexName) {
    LuceneService luceneService = LuceneServiceProvider.get(getCache());

    // Verify the index itself no longer exists
    assertNull(luceneService.getIndex(indexName, REGION_NAME));

    // Verify the underlying files region no longer exists
    String filesRegionName = LuceneServiceImpl.getUniqueIndexRegionName(indexName, REGION_NAME,
        LuceneIndexForPartitionedRegion.FILES_REGION_SUFFIX);
    assertNull(getCache().getRegion(filesRegionName));

    // Verify the underlying chunks region no longer exists
    String chunksRegionName = LuceneServiceImpl.getUniqueIndexRegionName(indexName, REGION_NAME,
        LuceneIndexForPartitionedRegion.CHUNKS_REGION_SUFFIX);
    assertNull(getCache().getRegion(chunksRegionName));

    // Verify the underlying AsyncEventQueue no longer exists
    String aeqId = LuceneServiceImpl.getUniqueIndexName(indexName, REGION_NAME);
    assertNull(getCache().getAsyncEventQueue(aeqId));

    // Verify the data region extension no longer exists
    LocalRegion region = (LocalRegion) getCache().getRegion(REGION_NAME);
    assertFalse(region.getExtensionPoint().getExtensions().iterator().hasNext());
  }

  private void exportData(RegionTestableType regionType) throws Exception {
    Region region = getCache().getRegion(REGION_NAME);
    RegionSnapshotService service = region.getSnapshotService();
    service.save(getSnapshotFile(getDiskDirs()[0], regionType),
        SnapshotOptions.SnapshotFormat.GEMFIRE);
  }

  private void importData(RegionTestableType regionType, int expectedRegionSize) throws Exception {
    Region region = getCache().getRegion(REGION_NAME);
    RegionSnapshotService service = region.getSnapshotService();
    SnapshotOptions options = service.createOptions();
    options.invokeCallbacks(true);
    service.load(getSnapshotFile(getDiskDirs()[0], regionType),
        SnapshotOptions.SnapshotFormat.GEMFIRE, options);
    assertEquals(expectedRegionSize, region.size());
  }

  private File getSnapshotFile(File baseDirectory, RegionTestableType regionType) {
    return new File(baseDirectory, REGION_NAME + "_" + regionType.name() + "_snapshot.gfd");
  }
}
