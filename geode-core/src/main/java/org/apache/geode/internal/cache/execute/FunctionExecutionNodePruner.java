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

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.partitioned.BucketId;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class FunctionExecutionNodePruner {
  public static final Logger logger = LogService.getLogger();

  public static HashMap<InternalDistributedMember, Set<BucketId>> pruneNodes(
      PartitionedRegion pr, Set<BucketId> buckets) {

    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (isDebugEnabled) {
      logger.debug("FunctionExecutionNodePruner: The buckets to be pruned are: {}", buckets);
    }

    final RegionAdvisor regionAdvisor = pr.getRegionAdvisor();

    final HashMap<InternalDistributedMember, Set<BucketId>> nodeToBucketsMap = new HashMap<>();
    try {
      for (BucketId bucketId : buckets) {
        Set<InternalDistributedMember> nodes = regionAdvisor.getBucketOwners(bucketId);
        if (nodes.isEmpty()) {
          if (isDebugEnabled) {
            logger.debug(
                "FunctionExecutionNodePruner: The buckets owners of the bucket: {} are empty, double check if they are all offline",
                bucketId);
          }
          nodes.add(pr.getOrCreateNodeForBucketRead(bucketId));
        }

        if (isDebugEnabled) {
          logger.debug("FunctionExecutionNodePruner: The buckets owners of the bucket: {} are: {}",
              bucketId, nodes);
        }
        for (InternalDistributedMember node : nodes) {
          nodeToBucketsMap.computeIfAbsent(node, k -> new HashSet<>(buckets.size())).add(bucketId);
        }
      }
    } catch (NoSuchElementException ignored) {
    }
    if (isDebugEnabled) {
      logger.debug("FunctionExecutionNodePruner: The node to buckets map is: {}", nodeToBucketsMap);
    }

    final Set<BucketId> currentBucketArray = new HashSet<>(buckets.size());

    /*
     * First Logic: Just implement the Greedy algorithm where you keep adding nodes which has the
     * biggest set of non-currentBucketSet. // Deterministic but it (almost)always chooses minimum
     * no of nodes to execute the function on.
     *
     * Second Logic: Give highest preference to the local node and after that use First Logic. //
     * Local Node gets preference but still it's deterministic for all the execution taking // place
     * at that node which require same set of buckets.
     *
     * Third Logic: After including local node, choose random nodes among the remaining nodes in
     * step until your currentBucketSet has all the required buckets. // No optimization for number
     * of nodes to execute the function
     */


    final InternalDistributedMember localNode = regionAdvisor.getDistributionManager().getId();
    final HashMap<InternalDistributedMember, Set<BucketId>> prunedNodeToBucketsMap =
        new HashMap<>();
    if (nodeToBucketsMap.get(localNode) != null) {
      Set<BucketId> bucketArray = nodeToBucketsMap.get(localNode);
      if (isDebugEnabled) {
        logger.debug(
            "FunctionExecutionNodePruner: Adding the node: {} which is local and buckets {} to prunedMap",
            localNode, bucketArray);
      }
      currentBucketArray.addAll(bucketArray);
      prunedNodeToBucketsMap.put(localNode, bucketArray);
      nodeToBucketsMap.remove(localNode);
    }
    while (!buckets.equals(currentBucketArray)) {
      if (nodeToBucketsMap.size() == 0) {
        break;
      }
      // continue
      InternalDistributedMember node =
          findNextNode(nodeToBucketsMap.entrySet(), currentBucketArray);
      if (node == null) {
        if (isDebugEnabled) {
          logger.debug(
              "FunctionExecutionNodePruner: Breaking out of prunedMap calculation due to no available nodes for remaining buckets");
        }
        break;
      }
      final Set<BucketId> bucketArray = nodeToBucketsMap.get(node);
      bucketArray.removeAll(currentBucketArray);
      if (!bucketArray.isEmpty()) {
        currentBucketArray.addAll(bucketArray);
        prunedNodeToBucketsMap.put(node, bucketArray);
        if (isDebugEnabled) {
          logger.debug(
              "FunctionExecutionNodePruner: Adding the node: {} and buckets {} to prunedMap", node,
              bucketArray);
        }
      }
      nodeToBucketsMap.remove(node);
    }
    if (isDebugEnabled) {
      logger.debug("FunctionExecutionNodePruner: The final prunedNodeToBucket calculated is: {}",
          prunedNodeToBucketsMap);
    }
    return prunedNodeToBucketsMap;
  }


  private static InternalDistributedMember findNextNode(
      final Set<Map.Entry<InternalDistributedMember, Set<BucketId>>> entrySet,
      final Set<BucketId> currentBucketArray) {

    InternalDistributedMember node = null;
    int max = -1;
    final List<InternalDistributedMember> nodesOfEqualSize = new ArrayList<>();

    for (final Map.Entry<InternalDistributedMember, Set<BucketId>> entry : entrySet) {
      final Set<BucketId> buckets = entry.getValue();
      final Set<BucketId> tempBuckets = new HashSet<>(buckets);
      tempBuckets.removeAll(currentBucketArray);

      final int size = tempBuckets.size();
      if (max < size) {
        max = size;
        node = entry.getKey();
        nodesOfEqualSize.clear();
        nodesOfEqualSize.add(node);
      } else if (max == size) {
        nodesOfEqualSize.add(node);
      }
    }

    // return node;
    return (nodesOfEqualSize.size() > 0
        ? nodesOfEqualSize.get(PartitionedRegion.RANDOM.nextInt(nodesOfEqualSize.size())) : null);
  }

  public static <K> Map<BucketId, Set<K>> groupByBucket(PartitionedRegion pr, Set<K> routingKeys,
      final boolean primaryMembersNeeded, final boolean hasRoutingObjects,
      final boolean isBucketSetAsFilter) {
    HashMap<BucketId, Set<K>> bucketToKeysMap = new HashMap<>();

    for (final K routingKey : routingKeys) {
      final BucketId bucketId;
      if (isBucketSetAsFilter) {
        bucketId = BucketId.valueOf((Integer) routingKey);
      } else {
        if (hasRoutingObjects) {
          bucketId = PartitionedRegionHelper.getBucket(pr, routingKey);
        } else {
          bucketId = PartitionedRegionHelper.getBucket(pr,
              Operation.FUNCTION_EXECUTION, routingKey, null, null);
        }
      }
      final InternalDistributedMember mem;
      if (primaryMembersNeeded) {
        mem = pr.getOrCreateNodeForBucketWrite(bucketId, null);
      } else {
        mem = pr.getOrCreateNodeForBucketRead(bucketId);
      }
      if (mem == null) {
        throw new FunctionException(format("No target node found for KEY, %s", routingKey));
      }
      bucketToKeysMap.computeIfAbsent(bucketId, k -> new HashSet<>()).add(routingKey);
    }
    return bucketToKeysMap;
  }

  public static <K> Set<BucketId> getBucketSet(PartitionedRegion pr, Set<K> routingKeys,
      final boolean hasRoutingObjects, boolean isBucketSetAsFilter) {
    Set<BucketId> buckets = null;
    for (K key : routingKeys) {
      final BucketId bucketId;
      if (isBucketSetAsFilter) {
        bucketId = (BucketId) key;
      } else {
        if (hasRoutingObjects) {
          bucketId = PartitionedRegionHelper.getBucket(pr, key);
        } else {
          bucketId =
              PartitionedRegionHelper.getBucket(pr, Operation.FUNCTION_EXECUTION, key, null, null);
        }
      }
      if (buckets == null) {
        buckets = new HashSet<>(routingKeys.size());
      }
      buckets.add(bucketId);
    }
    return buckets;
  }

  public static Map<InternalDistributedMember, Set<BucketId>> groupByMemberToBuckets(
      PartitionedRegion pr, Set<BucketId> bucketSet, boolean primaryOnly) {
    if (primaryOnly) {
      final HashMap<InternalDistributedMember, Set<BucketId>> memberToBucketsMap = new HashMap<>();
      try {
        for (BucketId bucketId : bucketSet) {
          final InternalDistributedMember mem = pr.getOrCreateNodeForBucketWrite(bucketId, null);
          memberToBucketsMap.computeIfAbsent(mem, k -> new HashSet<>(bucketSet.size()))
              .add(bucketId);
        }
      } catch (NoSuchElementException ignored) {
      }
      return memberToBucketsMap;
    } else {
      return pruneNodes(pr, bucketSet);
    }
  }

}
