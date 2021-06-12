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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketAdvisor.BucketProfile;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor.PartitionProfile;
import org.apache.geode.internal.util.VersionedArrayList;

/**
 * This class is an integration test for {@link PartitionedRegionQueryEvaluator} class.
 */
public class PartitionedRegionQueryEvaluatorIntegrationTest {

  @Rule
  public TestName name = new TestName();

  /**
   * Test for the helper method getNodeToBucketMap.
   */
  @Test
  public void testGetNodeToBucketMap() {
    int totalNodes = 100;
    String prPrefix = name.getMethodName();
    String localMaxMemory = "0";
    final int redundancy = 1;
    final int totalNoOfBuckets = 5;
    PartitionedRegion pr = (PartitionedRegion) PartitionedRegionTestHelper
        .createPartitionedRegion(prPrefix, localMaxMemory, redundancy);

    HashSet<Integer> bucketsToQuery = new HashSet<Integer>();
    for (int i = 0; i < totalNoOfBuckets; i++) {
      bucketsToQuery.add(i);
    }
    final String expectedUnknownHostException = UnknownHostException.class.getName();
    pr.getCache().getLogger().info(
        "<ExpectedException action=add>" + expectedUnknownHostException + "</ExpectedException>");
    final ArrayList nodes = createNodeList(totalNodes);
    pr.getCache().getLogger().info("<ExpectedException action=remove>"
        + expectedUnknownHostException + "</ExpectedException>");
    // populating bucket2Node of the partition region
    // ArrayList<InternalDistributedMember>
    final ArrayList dses = createDataStoreList(totalNodes);
    populateBucket2Node(pr, dses, totalNoOfBuckets);

    populateAllPartitionedRegion(pr, nodes);

    // running the algorithm and getting the list of bucktes to grab
    PartitionedRegionQueryEvaluator evalr =
        new PartitionedRegionQueryEvaluator(pr.getSystem(), pr, null, null, null, null,
            bucketsToQuery);
    Map n2bMap = null;
    try {
      n2bMap = evalr.buildNodeToBucketMap();
    } catch (Exception ex) {

    }
    ArrayList buckList = new ArrayList();
    for (Iterator itr = n2bMap.entrySet().iterator(); itr.hasNext();) {
      Map.Entry entry = (Map.Entry) itr.next();
      if (entry.getValue() != null) {
        buckList.addAll((List) entry.getValue());
      }
    }
    // checking size of the two lists
    assertEquals("Unexpected number of buckets", totalNoOfBuckets, buckList.size());
    for (int i = 0; i < totalNoOfBuckets; i++) {
      assertTrue(" Bucket with Id = " + i + " not present in bucketList.",
          buckList.contains(new Integer(i)));
    }

    pr.destroyRegion();
  }

  /**
   * This function populates bucket2Node region of the partition region
   */
  private void populateBucket2Node(PartitionedRegion pr, List nodes, int numOfBuckets) {
    assertEquals(0, pr.getRegionAdvisor().getCreatedBucketsCount());
    final RegionAdvisor ra = pr.getRegionAdvisor();
    int nodeListCnt = 0;
    Random ran = new Random();
    HashMap verMap = new HashMap(); // Map tracking version for profile insertion purposes
    for (int i = 0; i < numOfBuckets; i++) {
      nodeListCnt = setNodeListCnt(nodeListCnt);
      for (int j = 0; j < nodeListCnt; j++) {
        BucketProfile bp = new BucketProfile();
        bp.peerMemberId = (InternalDistributedMember) nodes.get(ran.nextInt(nodes.size()));
        Integer v;
        if ((v = (Integer) verMap.get(bp.getDistributedMember())) != null) {
          bp.version = v.intValue() + 1;
          verMap.put(bp.getDistributedMember(), new Integer(bp.version));
        } else {
          verMap.put(bp.getDistributedMember(), new Integer(0));
          bp.version = 0;
        }

        bp.isHosting = true;
        if (j == 0) {
          bp.isPrimary = true;
        }
        bp.scope = Scope.DISTRIBUTED_ACK;
        boolean forceBadProfile = true;
        assertTrue(ra.getBucket(i).getBucketAdvisor().putProfile(bp, forceBadProfile));
      }
    }
  }

  /**
   * This function decides number of the nodes in the list of bucket2Node region
   */
  private int setNodeListCnt(int i) {
    int nListcnt = 0;
    switch (i) {
      case 0:
        nListcnt = 1;
        break;
      case 1:
        nListcnt = 4;
        break;
      case 2:
        nListcnt = 1;
        break;
      case 3:
        nListcnt = 2;
        break;
      case 4:
        nListcnt = 1;
        break;
      case 5:
        nListcnt = 3;
        break;
      case 6:
        nListcnt = 3;
        break;
      case 7:
        nListcnt = 1;
        break;
      case 8:
        nListcnt = 1;
        break;
      case 9:
        nListcnt = 2;
        break;
    }
    return nListcnt;
  }

  /**
   * This functions number of new nodes specified by nCount.
   */
  private ArrayList createNodeList(int nCount) {
    ArrayList nodeList = new ArrayList(nCount);
    for (int i = 0; i < nCount; i++) {
      nodeList.add(createNode(i));
    }
    return nodeList;
  }

  private ArrayList createDataStoreList(int nCount) {
    ArrayList nodeList = new ArrayList(nCount);
    for (int i = 0; i < nCount; i++) {
      nodeList.add(createDataStoreMember(i));
    }
    return nodeList;
  }

  private VersionedArrayList getVersionedNodeList(int nCount, List<Node> nodes) {
    VersionedArrayList nodeList = new VersionedArrayList(nCount);
    Random ran = new Random();
    for (int i = 0; i < nCount; i++) {
      nodeList.add(nodes.get(ran.nextInt(nodes.size())));
    }
    return nodeList;
  }

  private InternalDistributedMember createDataStoreMember(int i) {
    return new InternalDistributedMember("host" + i, 3033);
  }

  /**
   * this function creates new node.
   */
  private Node createNode(int i) {
    Node node = new Node(new InternalDistributedMember("host" + i, 3033), i);
    node.setPRType(Node.DATASTORE);
    return node;
  }

  private void populateAllPartitionedRegion(PartitionedRegion pr, List nodes) {
    Region rootReg = PartitionedRegionHelper.getPRRoot(pr.getCache());
    PartitionRegionConfig prConf = new PartitionRegionConfig(pr.getPRId(), pr.getFullPath(),
        pr.getPartitionAttributes(), pr.getScope(), new EvictionAttributesImpl(),
        new ExpirationAttributes(), new ExpirationAttributes(), new ExpirationAttributes(),
        new ExpirationAttributes(), Collections.emptySet());
    RegionAdvisor ra = pr.getRegionAdvisor();
    for (Iterator itr = nodes.iterator(); itr.hasNext();) {
      Node n = (Node) itr.next();
      prConf.addNode(n);
      PartitionProfile pp = (PartitionProfile) ra.createProfile();
      pp.peerMemberId = n.getMemberId();
      pp.isDataStore = true;
      final boolean forceFakeProfile = true;
      pr.getRegionAdvisor().putProfile(pp, forceFakeProfile);
    }
    rootReg.put(pr.getRegionIdentifier(), prConf);
  }
}
