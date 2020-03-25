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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketSetHelper;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class FunctionExecutionNodePruner {
  public static final Logger logger = LogService.getLogger();

  public static HashMap<InternalDistributedMember, int[]> pruneNodes(
      PartitionedRegion pr, Set<Integer> buckets) {

    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (isDebugEnabled) {
      logger.debug("FunctionExecutionNodePruner: The buckets to be pruned are: {}", buckets);
    }
    HashMap<InternalDistributedMember, int[]> nodeToBucketsMap =
        new HashMap();
    HashMap<InternalDistributedMember, int[]> prunedNodeToBucketsMap =
        new HashMap();

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
            int[] bucketArray = new int[buckets.size() + 1];
            bucketArray[0] = 0;
            BucketSetHelper.add(bucketArray, bucketId);
            nodeToBucketsMap.put(node, bucketArray);
          } else {
            int[] bucketArray = nodeToBucketsMap.get(node);
            BucketSetHelper.add(bucketArray, bucketId);
            // nodeToBucketsMap.put(node, bucketSet);
          }
        }
      }
    } catch (NoSuchElementException e) {
    }
    if (isDebugEnabled) {
      logger.debug("FunctionExecutionNodePruner: The node to buckets map is: {}", nodeToBucketsMap);
    }
    int[] currentBucketArray = new int[buckets.size() + 1];
    currentBucketArray[0] = 0;

    /*
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
      int[] bucketArray = nodeToBucketsMap.get(localNode);
      if (isDebugEnabled) {
        logger.debug(
            "FunctionExecutionNodePruner: Adding the node: {} which is local and buckets {} to prunedMap",
            localNode, bucketArray);
      }
      System.arraycopy(bucketArray, 0, currentBucketArray, 0, bucketArray[0] + 1);
      prunedNodeToBucketsMap.put(localNode, bucketArray);
      nodeToBucketsMap.remove(localNode);
    }
    while (!arrayAndSetAreEqual(buckets, currentBucketArray)) {
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
      int[] bucketArray = nodeToBucketsMap.get(node);
      bucketArray = removeAllElements(bucketArray, currentBucketArray);
      if (BucketSetHelper.length(bucketArray) != 0) {
        currentBucketArray = addAllElements(currentBucketArray, bucketArray);
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
      Set<Map.Entry<InternalDistributedMember, int[]>> entrySet,
      int[] currentBucketArray) {

    InternalDistributedMember node = null;
    int max = -1;
    List<InternalDistributedMember> nodesOfEqualSize = new ArrayList<>();

    for (Map.Entry<InternalDistributedMember, int[]> entry : entrySet) {
      int[] buckets = entry.getValue();
      int[] tempbuckets = new int[buckets.length];
      System.arraycopy(buckets, 0, tempbuckets, 0, buckets[0] + 1);
      tempbuckets = removeAllElements(tempbuckets, currentBucketArray);

      if (max < BucketSetHelper.length(tempbuckets)) {
        max = BucketSetHelper.length(tempbuckets);
        node = entry.getKey();
        nodesOfEqualSize.clear();
        nodesOfEqualSize.add(node);
      } else if (max == BucketSetHelper.length(tempbuckets)) {
        nodesOfEqualSize.add(node);
      }
    }

    // return node;
    return (nodesOfEqualSize.size() > 0
        ? nodesOfEqualSize.get(PartitionedRegion.RANDOM.nextInt(nodesOfEqualSize.size())) : null);
  }

  public static Map<Integer, Set> groupByBucket(PartitionedRegion pr, Set routingKeys,
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


  public static int[] getBucketSet(PartitionedRegion pr, Set routingKeys,
      final boolean hasRoutingObjects, boolean isBucketSetAsFilter) {
    int[] bucketArray = null;
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
      if (bucketArray == null) {
        bucketArray = new int[routingKeys.size() + 1];
        bucketArray[0] = 0;
      }
      BucketSetHelper.add(bucketArray, bucketId);
    }
    return bucketArray;
  }

  public static HashMap<InternalDistributedMember, int[]> groupByMemberToBuckets(
      PartitionedRegion pr, Set<Integer> bucketSet, boolean primaryOnly) {
    if (primaryOnly) {
      HashMap<InternalDistributedMember, int[]> memberToBucketsMap = new HashMap();
      try {
        for (Integer bucketId : bucketSet) {
          InternalDistributedMember mem = pr.getOrCreateNodeForBucketWrite(bucketId, null);
          int[] bucketArray = memberToBucketsMap.get(mem);
          if (bucketArray == null) {
            bucketArray = new int[bucketSet.size() + 1]; // faster if this was an ArrayList
            memberToBucketsMap.put(mem, bucketArray);
            bucketArray[0] = 0;
          }
          BucketSetHelper.add(bucketArray, bucketId);

        }
      } catch (NoSuchElementException done) {
      }
      return memberToBucketsMap;
    } else {
      return pruneNodes(pr, bucketSet);
    }
  }

  private static boolean arrayAndSetAreEqual(Set<Integer> setA, int[] arrayB) {
    Set<Integer> setB = BucketSetHelper.toSet(arrayB);

    return setA.equals(setB);
  }

  private static int[] removeAllElements(int[] arrayA, int[] arrayB) {
    if (BucketSetHelper.length(arrayA) == 0 || BucketSetHelper.length(arrayB) == 0) {
      return arrayA;
    }

    Set<Integer> inSet = BucketSetHelper.toSet(arrayA);

    Set<Integer> subSet = BucketSetHelper.toSet(arrayB);

    inSet.removeAll(subSet);

    int[] outArray = BucketSetHelper.fromSet(inSet);

    return outArray;

  }

  private static int[] addAllElements(int[] arrayA, int[] arrayB) {
    if (BucketSetHelper.length(arrayB) == 0) {
      return arrayA;
    }

    Set<Integer> inSet = BucketSetHelper.toSet(arrayA);

    Set<Integer> addSet = BucketSetHelper.toSet(arrayB);

    inSet.addAll(addSet);

    int[] outArray = BucketSetHelper.fromSet(inSet);

    return outArray;

  }

}
