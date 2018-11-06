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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.logging.LogService;

public class FunctionExecutionNodePruner {
  public static final Logger logger = LogService.getLogger();

  public static HashMap<InternalDistributedMember, HashSet<Integer>> pruneNodes(
      PartitionedRegion pr, Set<Integer> buckets) {

    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (isDebugEnabled) {
      logger.debug("FunctionExecutionNodePruner: The buckets to be pruned are: {}", buckets);
    }
    HashMap<InternalDistributedMember, HashSet<Integer>> nodeToBucketsMap =
        new HashMap<InternalDistributedMember, HashSet<Integer>>();
    HashMap<InternalDistributedMember, HashSet<Integer>> prunedNodeToBucketsMap =
        new HashMap<InternalDistributedMember, HashSet<Integer>>();
    try {
      for (Integer bucketId : buckets) {
        Set<InternalDistributedMember> nodes = pr.getRegionAdvisor().getBucketOwners(bucketId);
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
          if (nodeToBucketsMap.get(node) == null) {
            HashSet<Integer> bucketSet = new HashSet<Integer>();
            bucketSet.add(bucketId);
            nodeToBucketsMap.put(node, bucketSet);
          } else {
            HashSet<Integer> bucketSet = nodeToBucketsMap.get(node);
            bucketSet.add(bucketId);
            nodeToBucketsMap.put(node, bucketSet);
          }
        }
      }
    } catch (NoSuchElementException e) {
    }
    if (isDebugEnabled) {
      logger.debug("FunctionExecutionNodePruner: The node to buckets map is: {}", nodeToBucketsMap);
    }
    HashSet<Integer> currentBucketSet = new HashSet<Integer>();

    /**
     * First Logic: Just implement the Greedy algorithm where you keep adding nodes which has the
     * biggest set of non-currentBucketSet. // Deterministic but it (almost)always chooses minimum
     * no of nodes to execute the function on.
     *
     * Second Logic: Give highest preference to the local node and after that use First Logic. //
     * Local Node gets preference but still its deterministic for all the execution taking // place
     * at that node which require same set of buckets.
     *
     * Third Logic: After including local node, choose random nodes among the remaining nodes in
     * step until your curentBucketSet has all the required buckets. // No optimization for number
     * of nodes to execute the function
     */


    InternalDistributedMember localNode = pr.getRegionAdvisor().getDistributionManager().getId();
    if (nodeToBucketsMap.get(localNode) != null) {
      HashSet<Integer> bucketSet = nodeToBucketsMap.get(localNode);
      if (isDebugEnabled) {
        logger.debug(
            "FunctionExecutionNodePruner: Adding the node: {} which is lcoal and buckets {} to prunedMap",
            localNode, bucketSet);
      }
      currentBucketSet.addAll(bucketSet);
      prunedNodeToBucketsMap.put(localNode, bucketSet);
      nodeToBucketsMap.remove(localNode);
    }
    while (!currentBucketSet.equals(buckets)) {
      if (nodeToBucketsMap.size() == 0) {
        break;
      }
      InternalDistributedMember node = findNextNode(nodeToBucketsMap.entrySet(), currentBucketSet);
      if (node == null) {
        if (isDebugEnabled) {
          logger.debug(
              "FunctionExecutionNodePruner: Breaking out of prunedMap calculation due to no available nodes for remaining buckets");
        }
        break;
      }
      HashSet<Integer> bucketSet = nodeToBucketsMap.get(node);
      bucketSet.removeAll(currentBucketSet);
      if (!bucketSet.isEmpty()) {
        currentBucketSet.addAll(bucketSet);
        prunedNodeToBucketsMap.put(node, bucketSet);
        if (isDebugEnabled) {
          logger.debug(
              "FunctionExecutionNodePruner: Adding the node: {} and buckets {} to prunedMap", node,
              bucketSet);
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
      Set<Map.Entry<InternalDistributedMember, HashSet<Integer>>> entrySet,
      HashSet<Integer> currentBucketSet) {

    InternalDistributedMember node = null;
    int max = -1;
    ArrayList<InternalDistributedMember> nodesOfEqualSize =
        new ArrayList<InternalDistributedMember>();
    for (Map.Entry<InternalDistributedMember, HashSet<Integer>> entry : entrySet) {
      HashSet<Integer> buckets = new HashSet<Integer>();
      buckets.addAll(entry.getValue());
      buckets.removeAll(currentBucketSet);

      if (max < buckets.size()) {
        max = buckets.size();
        node = entry.getKey();
        nodesOfEqualSize.clear();
        nodesOfEqualSize.add(node);
      } else if (max == buckets.size()) {
        nodesOfEqualSize.add(node);
      }
    }

    // return node;
    return (nodesOfEqualSize.size() > 0
        ? nodesOfEqualSize.get(PartitionedRegion.RANDOM.nextInt(nodesOfEqualSize.size())) : null);
  }

  public static HashMap<Integer, HashSet> groupByBucket(PartitionedRegion pr, Set routingKeys,
      final boolean primaryMembersNeeded, final boolean hasRoutingObjects,
      final boolean isBucketSetAsFilter) {
    HashMap bucketToKeysMap = new HashMap();
    Iterator i = routingKeys.iterator();

    while (i.hasNext()) {
      final Integer bucketId;
      Object key = i.next();
      if (isBucketSetAsFilter) {
        bucketId = ((Integer) key);
      } else {
        if (hasRoutingObjects) {
          bucketId = Integer.valueOf(PartitionedRegionHelper.getHashKey(pr, key));
        } else {
          bucketId = Integer.valueOf(PartitionedRegionHelper.getHashKey(pr,
              Operation.FUNCTION_EXECUTION, key, null, null));
        }
      }
      InternalDistributedMember mem = null;
      if (primaryMembersNeeded) {
        mem = pr.getOrCreateNodeForBucketWrite(bucketId.intValue(), null);
      } else {
        mem = pr.getOrCreateNodeForBucketRead(bucketId.intValue());
      }
      if (mem == null) {
        throw new FunctionException(
            String.format("No target node found for KEY, %s",
                key));
      }
      HashSet bucketKeys = (HashSet) bucketToKeysMap.get(bucketId);
      if (bucketKeys == null) {
        bucketKeys = new HashSet(); // faster if this was an ArrayList
        bucketToKeysMap.put(bucketId, bucketKeys);
      }
      bucketKeys.add(key);
    }
    return bucketToKeysMap;
  }

  public static HashSet<Integer> getBucketSet(PartitionedRegion pr, Set routingKeys,
      final boolean hasRoutingObjects, boolean isBucketSetAsFilter) {
    HashSet<Integer> bucketSet = null;
    for (Object key : routingKeys) {
      final Integer bucketId;
      if (isBucketSetAsFilter) {
        bucketId = (Integer) key;
      } else {
        if (hasRoutingObjects) {
          bucketId = Integer.valueOf(PartitionedRegionHelper.getHashKey(pr, key));
        } else {
          bucketId = Integer.valueOf(PartitionedRegionHelper.getHashKey(pr,
              Operation.FUNCTION_EXECUTION, key, null, null));
        }
      }
      if (bucketSet == null) {
        bucketSet = new HashSet<Integer>();
      }
      bucketSet.add(bucketId);
    }
    return bucketSet;
  }

  public static HashMap<InternalDistributedMember, HashSet<Integer>> groupByMemberToBuckets(
      PartitionedRegion pr, Set<Integer> bucketSet, boolean primaryOnly) {
    if (primaryOnly) {
      HashMap<InternalDistributedMember, HashSet<Integer>> memberToBucketsMap = new HashMap();
      try {
        for (Integer bucketId : bucketSet) {
          InternalDistributedMember mem = pr.getOrCreateNodeForBucketWrite(bucketId, null);
          HashSet buckets = memberToBucketsMap.get(mem);
          if (buckets == null) {
            buckets = new HashSet(); // faster if this was an ArrayList
            memberToBucketsMap.put(mem, buckets);
          }
          buckets.add(bucketId);
        }
      } catch (NoSuchElementException done) {
      }
      return memberToBucketsMap;
    } else {
      return pruneNodes(pr, bucketSet);
    }
  }
}
