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
package org.apache.geode.cache.lucene;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.util.Set;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.lucene.internal.LuceneIndexFactoryImpl;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.partition.PartitionMemberInfo;
import org.apache.geode.cache.partition.PartitionRebalanceInfo;
import org.apache.geode.cache.partition.PartitionRegionInfo;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
@RunWith(JUnitParamsRunner.class)
public class RebalanceWithRedundancyWithRegionCreatedBeforeReindexDUnitTest
    extends LuceneQueriesAccessorBase {

  private static final Logger logger = LogService.getLogger();

  protected VM dataStore3;
  protected VM dataStore4;

  public void postSetUp() throws Exception {
    super.postSetUp();
    dataStore3 = Host.getHost(0).getVM(2);
    dataStore4 = Host.getHost(0).getVM(3);
  }

  @Before
  public void setNumBuckets() {
    NUM_BUCKETS = 113;
  }

  @Before
  public void setLuceneReindexFlag() {
    dataStore1.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
    dataStore2.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
    dataStore3.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
    dataStore4.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = true);
  }

  @Override
  protected RegionTestableType[] getListOfRegionTestTypes() {
    return new RegionTestableType[] {RegionTestableType.PARTITION,
        RegionTestableType.PARTITION_REDUNDANT};
  }

  @After
  public void clearLuceneReindexFlag() {
    dataStore1.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = false);
    dataStore2.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = false);
    dataStore3.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = false);
    dataStore4.invoke(() -> LuceneServiceImpl.LUCENE_REINDEX = false);
  }

  protected SerializableRunnable createIndex = new SerializableRunnable("createIndex") {
    public void run() {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      ((LuceneIndexFactoryImpl) luceneService.createIndexFactory()).addField("text")
          .create(INDEX_NAME, REGION_NAME, LuceneServiceImpl.LUCENE_REINDEX);
    }
  };

  protected SerializableRunnable rebalance = new SerializableRunnable("rebalance") {
    public void run() throws InterruptedException {
      Cache cache = getCache();
      cache.getRegion(REGION_NAME);

      ResourceManager resMan = cache.getResourceManager();
      RebalanceFactory factory = resMan.createRebalanceFactory();
      logger.info("Starting rebalance");
      RebalanceResults rebalanceResults = null;
      RebalanceOperation rebalanceOp = factory.start();
      rebalanceResults = rebalanceOp.getResults();
      await().until(() -> rebalanceOp.isDone());
      logger.info("Rebalance completed: "
          + RebalanceResultsToString(rebalanceResults, "Rebalance completed"));
    }
  };

  protected SerializableRunnable doConcOps = new SerializableRunnable("doConcOps") {
    public void run() {
      putEntryInEachBucket(113);
    }
  };

  protected void createIndexAndRebalance(RegionTestableType regionTestType,
      SerializableRunnableIF createIndex, boolean doOps) throws Exception {

    // give rebalance some work to do by adding another vm
    // dataStore4.invoke(() -> (createIndex));
    dataStore4.invoke(() -> initDataStore(regionTestType));

    AsyncInvocation aiRebalancer = dataStore1.invokeAsync(rebalance);

    if (doOps) {
      AsyncInvocation aiConcOps = dataStore1.invokeAsync(doConcOps);
      aiConcOps.join();
      aiConcOps.checkException();
    }

    // re-index stored data
    AsyncInvocation ai1 = dataStore1.invokeAsync(createIndex);
    AsyncInvocation ai2 = dataStore2.invokeAsync(createIndex);
    AsyncInvocation ai3 = dataStore3.invokeAsync(createIndex);
    AsyncInvocation ai4 = dataStore4.invokeAsync(createIndex);

    aiRebalancer.join();
    aiRebalancer.checkException();

    ai1.join();
    ai2.join();
    ai3.join();
    ai4.join();

    ai1.checkException();
    ai2.checkException();
    ai3.checkException();
    ai4.checkException();

  }

  @Test
  @Parameters(method = "getListOfRegionTestTypes")
  public void returnCorrectResultsWithConcurrentOpsAndRebalance(RegionTestableType regionTestType)
      throws Exception {

    createAndPopulateRegion(regionTestType, NUM_BUCKETS / 2);

    createIndexAndRebalance(regionTestType, createIndex, true);

    waitForFlushBeforeExecuteTextSearch(dataStore3, 60000);
    executeTextSearch(dataStore3, "world", "text", NUM_BUCKETS);

  }

  private void createAndPopulateRegion(RegionTestableType regionTestType, int numEntries) {

    dataStore1.invoke(() -> initDataStore(regionTestType));
    dataStore2.invoke(() -> initDataStore(regionTestType));
    dataStore3.invoke(() -> initDataStore(regionTestType));

    putEntryInEachBucket(numEntries);
  }

  protected void putEntryInEachBucket(int numBuckets) {
    dataStore3.invoke(() -> {
      final Cache cache = getCache();
      Region<Object, Object> region = cache.getRegion(REGION_NAME);
      IntStream.range(0, numBuckets).forEach(i -> region.put(i, new TestObject("hello world")));
    });
  }

  public static String RebalanceResultsToString(RebalanceResults results, String title) {
    if (results == null) {
      return "null";
    }
    StringBuffer aStr = new StringBuffer();
    aStr.append("Rebalance results (" + title + ") totalTime: "
        + valueToString(results.getTotalTime()) + "\n");

    // bucketCreates
    aStr.append(
        "totalBucketCreatesCompleted: " + valueToString(results.getTotalBucketCreatesCompleted()));
    aStr.append(" totalBucketCreateBytes: " + valueToString(results.getTotalBucketCreateBytes()));
    aStr.append(
        " totalBucketCreateTime: " + valueToString(results.getTotalBucketCreateTime()) + "\n");

    // bucketTransfers
    aStr.append("totalBucketTransfersCompleted: "
        + valueToString(results.getTotalBucketTransfersCompleted()));
    aStr.append(
        " totalBucketTransferBytes: " + valueToString(results.getTotalBucketTransferBytes()));
    aStr.append(
        " totalBucketTransferTime: " + valueToString(results.getTotalBucketTransferTime()) + "\n");

    // primaryTransfers
    aStr.append("totalPrimaryTransfersCompleted: "
        + valueToString(results.getTotalPrimaryTransfersCompleted()));
    aStr.append(" totalPrimaryTransferTime: " + valueToString(results.getTotalPrimaryTransferTime())
        + "\n");

    // PartitionRebalanceDetails (per region)
    Set<PartitionRebalanceInfo> prdSet = results.getPartitionRebalanceDetails();
    for (PartitionRebalanceInfo prd : prdSet) {
      aStr.append(partitionRebalanceDetailsToString(prd));
    }
    aStr.append("total time (ms): " + valueToString(results.getTotalTime()));

    String returnStr = aStr.toString();
    return returnStr;
  }

  private static String partitionRebalanceDetailsToString(PartitionRebalanceInfo details) {
    if (details == null) {
      return "null\n";
    }

    StringBuffer aStr = new StringBuffer();
    aStr.append("PartitionedRegionDetails for region named " + getRegionName(details) + " time: "
        + valueToString(details.getTime()) + "\n");

    // bucketCreates
    aStr.append("bucketCreatesCompleted: " + valueToString(details.getBucketCreatesCompleted()));
    aStr.append(" bucketCreateBytes: " + valueToString(details.getBucketCreateBytes()));
    aStr.append(" bucketCreateTime: " + valueToString(details.getBucketCreateTime()) + "\n");

    // bucketTransfers
    aStr.append(
        "bucketTransfersCompleted: " + valueToString(details.getBucketTransfersCompleted()));
    aStr.append(" bucketTransferBytes: " + valueToString(details.getBucketTransferBytes()));
    aStr.append(" bucketTransferTime: " + valueToString(details.getBucketTransferTime()) + "\n");

    // primaryTransfers
    aStr.append(
        "PrimaryTransfersCompleted: " + valueToString(details.getPrimaryTransfersCompleted()));
    aStr.append(" PrimaryTransferTime: " + valueToString(details.getPrimaryTransferTime()) + "\n");

    // PartitionMemberDetails (before)
    aStr.append("PartitionedMemberDetails (before)\n");
    Set<PartitionMemberInfo> pmdSet = details.getPartitionMemberDetailsBefore();
    for (PartitionMemberInfo pmd : pmdSet) {
      aStr.append(partitionMemberDetailsToString(pmd));
    }

    // PartitionMemberDetails (after)
    aStr.append("PartitionedMemberDetails (after)\n");
    pmdSet = details.getPartitionMemberDetailsAfter();
    for (PartitionMemberInfo pmd : pmdSet) {
      aStr.append(partitionMemberDetailsToString(pmd));
    }

    return aStr.toString();
  }

  public static String partitionedRegionDetailsToString(PartitionRegionInfo prd) {

    if (prd == null) {
      return "null\n";
    }

    StringBuffer aStr = new StringBuffer();

    aStr.append("PartitionedRegionDetails for region named " + getRegionName(prd) + "\n");
    aStr.append("  configuredBucketCount: " + valueToString(prd.getConfiguredBucketCount()) + "\n");
    aStr.append("  createdBucketCount: " + valueToString(prd.getCreatedBucketCount()) + "\n");
    aStr.append(
        "  lowRedundancyBucketCount: " + valueToString(prd.getLowRedundancyBucketCount()) + "\n");
    aStr.append(
        "  configuredRedundantCopies: " + valueToString(prd.getConfiguredRedundantCopies()) + "\n");
    aStr.append("  actualRedundantCopies: " + valueToString(prd.getActualRedundantCopies()) + "\n");

    // memberDetails
    Set<PartitionMemberInfo> pmd = prd.getPartitionMemberInfo();
    for (PartitionMemberInfo memberDetails : pmd) {
      aStr.append(partitionMemberDetailsToString(memberDetails));
    }

    // colocatedWithDetails
    String colocatedWith = prd.getColocatedWith();
    aStr.append("  colocatedWith: " + colocatedWith + "\n");

    String returnStr = aStr.toString();
    return returnStr;
  }

  private static String partitionMemberDetailsToString(PartitionMemberInfo pmd) {
    StringBuffer aStr = new StringBuffer();
    long localMaxMemory = pmd.getConfiguredMaxMemory();
    long size = pmd.getSize();
    aStr.append("    Member Details for: " + pmd.getDistributedMember() + "\n");
    aStr.append("      configuredMaxMemory: " + valueToString(localMaxMemory));
    double inUse = (double) size / localMaxMemory;
    double heapUtilization = inUse * 100;
    aStr.append(" size: " + size + " (" + valueToString(heapUtilization) + "%)");
    aStr.append(" bucketCount: " + valueToString(pmd.getBucketCount()));
    aStr.append(" primaryCount: " + valueToString(pmd.getPrimaryCount()) + "\n");
    return aStr.toString();
  }

  /**
   * Convert the given long to a String; if it is negative then flag it in the string
   */
  private static String valueToString(long value) {
    String returnStr = "" + value;
    return returnStr;
  }

  /**
   * Convert the given double to a String; if it is negative then flag it in the string
   */
  private static String valueToString(double value) {
    String returnStr = "" + value;
    return returnStr;
  }

  public static String getRegionName(PartitionRegionInfo prd) {
    return prd.getRegionPath().substring(1);
  }

  public static String getRegionName(PartitionRebalanceInfo prd) {
    return prd.getRegionPath().substring(1);
  }
}
