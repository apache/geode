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

import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.FetchKeysMessage;
import org.apache.geode.internal.cache.partitioned.FetchKeysMessage.FetchKeysResponse;
import org.apache.geode.internal.cache.partitioned.PRLocallyDestroyedException;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.logging.LogService;

/**
 * Extracted from {@link PartitionedRegion}. This is a utility used by Hydra test code only.
 */
public class PartitionedRegionGetSomeKeys {

  private static final Logger logger = LogService.getLogger();

  /**
   * Test Method: Get a random set of keys from a randomly selected bucket using the provided
   * {@code Random} number generator.
   *
   * @return A set of keys from a randomly chosen bucket or {@link Collections#EMPTY_SET}
   */
  public static Set<?> getSomeKeys(PartitionedRegion partitionedRegion, Random random)
      throws IOException, ClassNotFoundException {
    Set<Integer> bucketIdSet = partitionedRegion.getRegionAdvisor().getBucketSet();

    if (bucketIdSet != null && !bucketIdSet.isEmpty()) {
      Object[] bucketIds = bucketIdSet.toArray();
      Integer bucketId = null;
      Set<?> someKeys;

      // Randomly pick a node to get some data from
      for (int i = 0; i < bucketIds.length; i++) {
        try {
          int whichBucket = random.nextInt(bucketIds.length);
          if (whichBucket >= bucketIds.length) {
            // The GSRandom.nextInt(int) may return a value that includes the maximum.
            whichBucket = bucketIds.length - 1;
          }
          bucketId = (Integer) bucketIds[whichBucket];

          InternalDistributedMember member = partitionedRegion.getNodeForBucketRead(bucketId);
          if (member != null) {
            if (member.equals(partitionedRegion.getMyId())) {
              someKeys = partitionedRegion.getDataStore().handleRemoteGetKeys(bucketId,
                  InterestType.REGULAR_EXPRESSION, ".*", false);
            } else {
              FetchKeysResponse fetchKeysResponse =
                  FetchKeysMessage.send(member, partitionedRegion, bucketId, false);
              someKeys = fetchKeysResponse.waitForKeys();
            }

            if (someKeys != null && !someKeys.isEmpty()) {
              return someKeys;
            }
          }
        } catch (ForceReattemptException movinOn) {
          partitionedRegion.checkReadiness();
          logger.debug(
              "Test hook getSomeKeys caught a ForceReattemptException for bucketId={}{}{}. Moving on to another bucket",
              partitionedRegion.getPRId(), partitionedRegion.BUCKET_ID_SEPARATOR, bucketId,
              movinOn);
          continue;
        } catch (PRLocallyDestroyedException ignore) {
          logger.debug("getSomeKeys: Encountered PRLocallyDestroyedException");
          partitionedRegion.checkReadiness();
          continue;
        }
      }
    }
    return Collections.emptySet();
  }
}
