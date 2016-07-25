/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.BucketAdvisor.BucketProfile;
import com.gemstone.gemfire.internal.cache.partitioned.RegionAdvisor;
import com.gemstone.gemfire.internal.cache.partitioned.RegionAdvisor.PartitionProfile;
import com.gemstone.gemfire.internal.util.VersionedArrayList;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This class is an integration test for <code>PartitionedRegionQueryEvaluator</code> class.
 */
@Category(IntegrationTest.class)
public class PartitionedRegionQueryEvaluatorIntegrationTest
{
  @Rule public TestName name = new TestName();
  LogWriter logger = null;

  @Before
  public void setUp() throws Exception
  {
    if (logger == null) {
      logger = PartitionedRegionTestHelper.getLogger();
    }
  }

  /**
   * Test for the helper method getNodeToBucketMap.
   * 
   */
  @Test
  public void testGetNodeToBucketMap()
  {
    int totalNodes = 100;
    String prPrefix = name.getMethodName();
    String localMaxMemory = "0";
    final int redundancy = 1;
    final int totalNoOfBuckets = 5;
    PartitionedRegion pr = (PartitionedRegion)PartitionedRegionTestHelper
        .createPartitionedRegion(prPrefix, localMaxMemory, redundancy);

    HashSet<Integer> bucketsToQuery = new HashSet<Integer>();
    for (int i = 0; i < totalNoOfBuckets; i++) {
      bucketsToQuery.add(i);
    }
    final String expectedUnknownHostException = UnknownHostException.class
    .getName();
    pr.getCache().getLogger().info(
    "<ExpectedException action=add>" + expectedUnknownHostException
        + "</ExpectedException>");
    final ArrayList nodes = createNodeList(totalNodes);    
    pr.getCache().getLogger().info(
        "<ExpectedException action=remove>" + expectedUnknownHostException
            + "</ExpectedException>");
    // populating bucket2Node of the partition region
      // ArrayList<InternalDistributedMember>
    final ArrayList dses = createDataStoreList(totalNodes);
    populateBucket2Node(pr, dses, totalNoOfBuckets);  
  
    populateAllPartitionedRegion(pr, nodes);

    // running the algorithm and getting the list of bucktes to grab
    PartitionedRegionQueryEvaluator evalr = new PartitionedRegionQueryEvaluator(pr.getSystem(), pr, null, null, null, bucketsToQuery);
    Map n2bMap = null;
    try {
      n2bMap = evalr.buildNodeToBucketMap();
    } catch (Exception ex) {
      
    }
    ArrayList buckList = new ArrayList();
    for (Iterator itr = n2bMap.entrySet().iterator(); itr.hasNext();) {
      Map.Entry entry = (Map.Entry)itr.next();
      if (entry.getValue() != null)
        buckList.addAll((List)entry.getValue());
    }
    // checking size of the two lists
    assertEquals("Unexpected number of buckets", totalNoOfBuckets, buckList.size());
    for (int i = 0; i < totalNoOfBuckets; i++) {
      assertTrue(" Bucket with Id = " + i + " not present in bucketList.",
          buckList.contains(new Integer(i)));
    }
    clearAllPartitionedRegion(pr);
    logger.info("************test ended successfully **********");
    
  }

  /**
   * This function populates bucket2Node region of the partition region
   * 
   * @param pr
   */
  public void populateBucket2Node(PartitionedRegion pr, List nodes,
      int numOfBuckets)
  {
    assertEquals(0, pr.getRegionAdvisor().getCreatedBucketsCount());
    final RegionAdvisor ra = pr.getRegionAdvisor();
    int nodeListCnt = 0;
    Random ran = new Random();
    HashMap verMap = new HashMap(); // Map tracking version for profile insertion purposes
    for (int i = 0; i < numOfBuckets; i++) {
      nodeListCnt = setNodeListCnt(nodeListCnt);  
      for (int j = 0; j < nodeListCnt; j++) {
        BucketProfile bp = new BucketProfile();
        bp.peerMemberId = (InternalDistributedMember) 
            nodes.get(ran.nextInt(nodes.size()));
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
  
  private void clearAllPartitionedRegion(PartitionedRegion pr) {
    Cache cache = pr.getCache();
    Region allPR = PartitionedRegionHelper.getPRRoot(cache);
    allPR.clear();    
  }
  
  /**
   * This function decides number of the nodes in the list of bucket2Node region
   * 
   * @param i
   * @return
   */
  private int setNodeListCnt(int i)
  {
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
   * 
   * @param nCount
   * @return
   */
  private ArrayList createNodeList(int nCount)
  {
    ArrayList nodeList = new ArrayList(nCount);
    for (int i = 0; i < nCount; i++) {
      nodeList.add(createNode(i));
    }
    return nodeList;
  }

  private ArrayList createDataStoreList(int nCount)
  {
    // ArrayList<InternalDistributedMember>
    ArrayList nodeList = new ArrayList(nCount);
    for (int i = 0; i < nCount; i++) {
      nodeList.add(createDataStoreMember(i));
    }
    return nodeList;
  }

  private VersionedArrayList getVersionedNodeList(int nCount, List<Node> nodes)
  {
    VersionedArrayList nodeList = new VersionedArrayList(nCount);
    Random ran = new Random();
    for (int i = 0; i < nCount; i++) {
      nodeList.add(nodes.get(ran.nextInt(nodes.size())));
    }
    return nodeList;
  }

  private InternalDistributedMember createDataStoreMember(int i)
  {
    String hostname = null;
    InternalDistributedMember mem = null;
    try {
      mem = new InternalDistributedMember("host" + i, 3033);
    }
    catch (java.net.UnknownHostException uhe) {
      logger.severe("PartitionedRegion: initalizeNode() Unknown host = "
          + hostname + " servicePort = " + 0, uhe);
      throw new PartitionedRegionException(
          "PartitionedRegionDataStore: initalizeNode() Unknown host = "
              + hostname + " servicePort = " + 0, uhe);
    }
    return mem;
  }

  /**
   * this function creates new node.
   * 
   * @return
   */
  public Node createNode(int i)
  {
    Node node = null;
    try {
      node = new Node(new InternalDistributedMember("host" + i, 3033), i);
      node.setPRType(Node.DATASTORE);
    }
    catch (java.net.UnknownHostException uhe) {
      logger.severe("PartitionedRegion: initalizeNode() threw exception", uhe);
      throw new PartitionedRegionException("", uhe);
    }
    return node;
  }

  private void populateAllPartitionedRegion(PartitionedRegion pr, List nodes)
  {
    // int totalNodes = 4;
    Region rootReg = PartitionedRegionHelper.getPRRoot(pr.getCache());
//    Region allPRs = PartitionedRegionHelper.getPRConfigRegion(rootReg, pr
//        .getCache());
    PartitionRegionConfig prConf = new PartitionRegionConfig(pr.getPRId(), pr
        .getFullPath(), pr.getPartitionAttributes(), pr.getScope());
    RegionAdvisor ra = pr.getRegionAdvisor();
    for (Iterator itr = nodes.iterator(); itr.hasNext();) {
      Node n = (Node)itr.next();
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
