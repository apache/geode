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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.client.internal.ClientPartitionAdvisor;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.BucketServerLocation66;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;


/**
 * This class tests single-hop bulk operations in client caches. Single-hop makes use
 * of metadata concerning partitioned region bucket locations to find primary buckets
 * on which to operate. If the metadata is incorrect it forces scheduling of a refresh.
 * A total count of all refresh requests is kept in the metadata service and is used
 * by this test to verify whether the cache thought the metadata was correct or not.
 */
public class SingleHopGetAllPutAllDUnitTest extends PRClientServerTestBase {


  private static final long serialVersionUID = 3873751456134028508L;

  public SingleHopGetAllPutAllDUnitTest() {
    super();

  }

  @Before
  public void createScenario() {
    ArrayList commonAttributes =
        createCommonServerAttributes("TestPartitionedRegion", null, 2, 13, null);
    createClientServerScenarioSingleHop(commonAttributes, 20, 20, 20);
  }

  /**
   * populate the region, do a getAll, verify that metadata is fetched
   * do another getAll and verify that metadata did not need to be refetched
   */
  @Test
  public void testGetAllInClient() {
    client.invoke("testGetAllInClient", () -> {
      Region region = cache.getRegion(PartitionedRegionName);
      assertThat(region).isNotNull();
      final List testValueList = new ArrayList();
      final List testKeyList = new ArrayList();
      for (int i = (totalNumBuckets.intValue() * 3); i > 0; i--) {
        testValueList.add("execKey-" + i);
      }
      DistributedSystem.setThreadsSocketPolicy(false);
      int j = 0;
      Map origVals = new HashMap();
      for (Iterator i = testValueList.iterator(); i.hasNext();) {
        testKeyList.add(j);
        Integer key = new Integer(j++);
        Object val = i.next();
        origVals.put(key, val);
        region.put(key, val);
      }

      // check if the client meta-data is in synch
      verifyMetadata();

      long metadataRefreshes =
          ((GemFireCacheImpl) cache).getClientMetadataService()
              .getTotalRefreshTaskCount_TEST_ONLY();

      Map resultMap = region.getAll(testKeyList);
      assertThat(resultMap).isEqualTo(origVals);

      // a new refresh should not have been triggered
      assertThat(((GemFireCacheImpl) cache).getClientMetadataService()
          .getTotalRefreshTaskCount_TEST_ONLY())
              .isEqualTo(metadataRefreshes);
    });
  }


  /**
   * perform a putAll and ensure that metadata is fetched. Then do another
   * putAll and ensure that metadata did not need to be refreshed
   */
  @Test
  public void testPutAllInClient() {
    client.invoke("testPutAllInClient", () -> {
      Region<String, String> region = cache.getRegion(PartitionedRegionName);
      assertThat(region).isNotNull();

      Map<String, String> keysValuesMap = new HashMap<String, String>();
      List<String> testKeysList = new ArrayList<>();
      for (int i = (totalNumBuckets.intValue() * 3); i > 0; i--) {
        testKeysList.add("putAllKey-" + i);
        keysValuesMap.put("putAllKey-" + i, "values-" + i);
      }
      DistributedSystem.setThreadsSocketPolicy(false);

      region.putAll(keysValuesMap);

      verifyMetadata();

      long metadataRefreshes =
          ((GemFireCacheImpl) cache).getClientMetadataService()
              .getTotalRefreshTaskCount_TEST_ONLY();

      region.putAll(keysValuesMap);

      // a new refresh should not have been triggered
      assertThat(((GemFireCacheImpl) cache).getClientMetadataService()
          .getTotalRefreshTaskCount_TEST_ONLY())
              .isEqualTo(metadataRefreshes);
    });
  }

  /**
   * Do a putAll and ensure that metadata has been fetched. Then do a removeAll and
   * ensure that metadata did not need to be refreshed. Finally do a getAll to ensure
   * that the removeAll did its job.
   */
  @Test
  public void testRemoveAllInClient() {
    client.invoke("testRemoveAllInClient", () -> {
      Region<String, String> region = cache.getRegion(PartitionedRegionName);
      assertThat(region).isNotNull();

      Map<String, String> keysValuesMap = new HashMap<String, String>();
      List<String> testKeysList = new ArrayList<String>();
      for (int i = (totalNumBuckets.intValue() * 3); i > 0; i--) {
        testKeysList.add("putAllKey-" + i);
        keysValuesMap.put("putAllKey-" + i, "values-" + i);
      }

      DistributedSystem.setThreadsSocketPolicy(false);

      region.putAll(keysValuesMap);

      verifyMetadata();

      long metadataRefreshes =
          ((GemFireCacheImpl) cache).getClientMetadataService()
              .getTotalRefreshTaskCount_TEST_ONLY();

      region.removeAll(testKeysList);

      // a new refresh should not have been triggered
      assertThat(((GemFireCacheImpl) cache).getClientMetadataService()
          .getTotalRefreshTaskCount_TEST_ONLY())
              .isEqualTo(metadataRefreshes);

      HashMap<String, Object> noValueMap = new HashMap<String, Object>();
      for (String key : testKeysList) {
        noValueMap.put(key, null);
      }

      assertThat(noValueMap).isEqualTo(region.getAll(testKeysList));

      assertThat(((GemFireCacheImpl) cache).getClientMetadataService()
          .getTotalRefreshTaskCount_TEST_ONLY())
              .isEqualTo(metadataRefreshes);
    });
  }

  /**
   * If a client doesn't know the primary location of a bucket it should perform a
   * metadata refresh. This test purposefully removes all primary location knowledge
   * from PR metadata in a client cache and then performs a bulk operation. This
   * should trigger a refresh.
   */
  @Test
  public void testBulkOpInClientWithBadMetadataCausesRefresh() {
    client.invoke("testBulkOpInClientWithBadMetadataCausesRefresh", () -> {
      Region region = cache.getRegion(PartitionedRegionName);
      assertThat(region).isNotNull();
      final List testValueList = new ArrayList();
      final List testKeyList = new ArrayList();
      for (int i = (totalNumBuckets.intValue() * 3); i > 0; i--) {
        testValueList.add("execKey-" + i);
      }
      DistributedSystem.setThreadsSocketPolicy(false);
      int j = 0;
      Map origVals = new HashMap();
      for (Iterator i = testValueList.iterator(); i.hasNext();) {
        testKeyList.add(j);
        Integer key = new Integer(j++);
        Object val = i.next();
        origVals.put(key, val);
        region.put(key, val);
      }

      // check if the client meta-data is in synch
      verifyMetadata();

      long metadataRefreshes =
          ((GemFireCacheImpl) cache).getClientMetadataService()
              .getTotalRefreshTaskCount_TEST_ONLY();

      removePrimaryMetadata();

      Map resultMap = region.getAll(testKeyList);
      assertThat(resultMap).isEqualTo(origVals);

      // a new refresh should have been triggered
      assertThat(((GemFireCacheImpl) cache).getClientMetadataService()
          .getTotalRefreshTaskCount_TEST_ONLY())
              .isNotEqualTo(metadataRefreshes);
    });
  }


  private void verifyMetadata() {
    Region region = cache.getRegion(PartitionedRegionName);
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion) region);

    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();

    await().until(() -> regionMetaData.size() > 0);
    assertThat(regionMetaData).containsKey(region.getFullPath());
    await().until(() -> {
      ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
      assertThat(prMetaData).isNotNull();
      assertThat(prMetaData.adviseRandomServerLocation()).isNotNull();
      return true;
    });
  }

  private void removePrimaryMetadata() {
    Region region = cache.getRegion(PartitionedRegionName);
    ClientMetadataService cms = ((GemFireCacheImpl) cache).getClientMetadataService();
    cms.getClientPRMetadata((LocalRegion) region);

    final Map<String, ClientPartitionAdvisor> regionMetaData = cms.getClientPRMetadata_TEST_ONLY();

    final ClientPartitionAdvisor prMetaData = regionMetaData.get(region.getFullPath());
    Map<Integer, List<BucketServerLocation66>> bucketLocations =
        prMetaData.getBucketServerLocationsMap_TEST_ONLY();
    for (Map.Entry<Integer, List<BucketServerLocation66>> locationEntry : bucketLocations
        .entrySet()) {
      List<BucketServerLocation66> newList = new ArrayList<>(locationEntry.getValue());
      for (Iterator<BucketServerLocation66> bucketIterator = newList.iterator(); bucketIterator
          .hasNext();) {
        BucketServerLocation66 location = bucketIterator.next();
        if (location.isPrimary()) {
          bucketIterator.remove();
        }
        bucketLocations.put(locationEntry.getKey(), newList);
      }

    }
  }

}
