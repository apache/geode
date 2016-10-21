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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.util.Set;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;

/**
 * Test to verify the meta-data cleanUp done at the time of cache close Op. This test creates 2
 * PRs(both data stores) on 2 VMs with redundantCopies "1". Put Ops are done on both the VMs. Each
 * bucket will be created in both the nodes as redundantCopies is 1.
 * 
 * 
 */
@Category(DistributedTest.class)
public class PartitionedRegionCacheCloseDUnitTest extends PartitionedRegionDUnitTestCase {

  ////// constructor //////////
  public PartitionedRegionCacheCloseDUnitTest() {
    super();
  }// end of constructor

  final static int MAX_REGIONS = 1;

  final int totalNumBuckets = 5;

  ////////// test methods ////////////////
  @Test
  public void testCacheClose() throws Exception, Throwable {
    final String rName = getUniqueName();

    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Create PRs On 2 VMs

    CacheSerializableRunnable createPRs = new CacheSerializableRunnable("createPrRegions") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          cache.createRegion(rName + i, createRegionAttributesForPR(1, 20));
        }
      }
    };

    // Create PRs on only 2 VMs
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);


    // Do put operations on these 3 PRs asynchronosly.
    AsyncInvocation async0, async1;
    async0 = vm0.invokeAsync(new CacheSerializableRunnable("doPutOperations") {
      public void run2() {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          PartitionedRegion pr = (PartitionedRegion) cache.getRegion(Region.SEPARATOR + rName + j);
          assertNotNull(pr);
          int numBuckets = pr.getTotalNumberOfBuckets();
          Integer key;
          for (int k = 0; k < numBuckets; k++) {
            key = new Integer(k);
            pr.put(key, rName + k);
          }
          org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
              .info("VM0 Done put successfully for PR = " + rName + j);
        }
      }
    });

    async1 = vm1.invokeAsync(new CacheSerializableRunnable("doPutOperations") {
      public void run2() {
        Cache cache = getCache();
        for (int j = 0; j < MAX_REGIONS; j++) {
          PartitionedRegion pr =
              (PartitionedRegion) cache.getRegion(Region.SEPARATOR + rName + (j));
          assertNotNull(pr);
          int numBuckets = pr.getTotalNumberOfBuckets();
          int max = numBuckets * 2;
          Integer key;
          for (int k = numBuckets; k < max; k++) {
            key = new Integer(k);
            pr.put(key, rName + k);
          }
          org.apache.geode.test.dunit.LogWriterUtils.getLogWriter()
              .info("VM1 Done put successfully for PR = " + rName + j);
        }
      }
    });
    ThreadUtils.join(async0, 30 * 1000);
    ThreadUtils.join(async1, 30 * 1000);

    if (async0.exceptionOccurred()) {
      Assert.fail("Exception during async0", async0.getException());
    }

    // Here we would close cache on one of the vms.
    CacheSerializableRunnable closeCache = new CacheSerializableRunnable("closeCache") {

      public void run2() {
        Cache cache = getCache();
        cache.close();
      }
    };

    // Here we would close cache on one of the vms.
    CacheSerializableRunnable validateCacheCloseCleanUp =
        new CacheSerializableRunnable("validateCacheCloseCleanUp") {

          public void run2() {

            Cache cache = getCache();
            LogWriter logger = cache.getLogger();

            final Region root = PartitionedRegionHelper.getPRRoot(cache);
            // Region allPr = PartitionedRegionHelper.getPRConfigRegion(root, cache);

            // Construct a BucketCacheListener that notifies when all buckets
            // entries
            // in the bucket2node regions (for each PartitionedRegion) have been
            // updated
            // This assumes that the default number of buckets is used per Region
            // final int totalBuckets = MAX_REGIONS
            // * PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
            // BucketCacheListener cl = new BucketCacheListener(totalBuckets);
            // for (Iterator confI = root.keySet().iterator(); confI.hasNext();) {
            // PartitionRegionConfig prConf = (PartitionRegionConfig)root.get(confI.next());
            // assertNotNull(prConf);
            // Region b2n = getBucket2Node(root, prConf.getPRId());
            // AttributesMutator am = b2n.getAttributesMutator();
            // am.addCacheListener(cl);
            // }
            // for (int j = 0; j < MAX_REGIONS; j++) {
            // String regionName = rName + j;
            // PartitionRegionConfig prConf = (PartitionRegionConfig)allPr.get(regionName);
            // assertNotNull(prConf);
            //
            // Region b2n = getBucket2Node(root, prConf.getPRId());
            // AttributesMutator am = b2n.getAttributesMutator();
            // am.addCacheListener(cl);
            // }
            // try {
            // int numWait = 0;
            // synchronized (cl) {
            // while (!cl.hasNotified && numWait < 5) {
            // getCache().getLogger().info("Waiting for updates to buckets");
            // cl.wait(1000);
            // numWait++;
            // }
            // if (cl.hasNotified) {
            // getCache().getLogger().info("Got notification to continue test");
            // }
            // }
            // }
            // catch (InterruptedException ie) {
            // throw new RuntimeException("Waiting for updates interrupted", ie);
            // }

            for (int j = 0; j < MAX_REGIONS; j++) {
              final String regionName = "#" + rName + j;

              Wait.waitForCriterion(new WaitCriterion() {

                private Set<Node> nodes;

                @Override
                public boolean done() {
                  // Verify PRConfig for each Region.
                  PartitionRegionConfig prConf = (PartitionRegionConfig) root.get(regionName);

                  nodes = prConf.getNodes();
                  return nodes.size() == 1;

                }

                @Override
                public String description() {
                  return "Expected 1 node, but found " + nodes;

                }
              }, 30000, 100, true);


              // Verify Bucket2Node Regions.
              // Region b2n = root
              // .getSubregion(PartitionedRegionHelper.BUCKET_2_NODE_TABLE_PREFIX
              // + prConf.getPRId());
              // assertNotNull(b2n);
              // logger.info("Size of b2n for region = " + regionName + " = "
              // + b2n.size());
              //
              // for (java.util.Iterator itr = b2n.values().iterator(); itr.hasNext();) {
              // VersionedArrayList nodeList = (VersionedArrayList)itr.next();
              // logger.info("Size of nodeList for b2n entries for region = "
              // + regionName + " = " + nodeList.size());
              // assertIndexDetailsEquals("Node list: " + nodeList, 1, nodeList.size());
              // }
            }
          }
        };

    // Close all the PRs on vm2
    vm0.invoke(closeCache);
    vm1.invoke(validateCacheCloseCleanUp);
  }

  // protected static Region getBucket2Node(Region root, int prid)
  // {
  // Region ret = null;
  // AttributesFactory factory = new AttributesFactory(root.getAttributes());
  // factory.setDataPolicy(DataPolicy.REPLICATE);
  // factory.setScope(Scope.DISTRIBUTED_ACK);
  // RegionAttributes regionAttr = factory.create();
  // try {
  // ret = ((LocalRegion)root)
  // .createSubregion(PartitionedRegionHelper.BUCKET_2_NODE_TABLE_PREFIX
  // + prid, regionAttr, new InternalRegionArguments().setIsUsedForPartitionedRegionAdmin(true));
  // }
  // catch (RegionExistsException ree) {
  // ret = root
  // .getSubregion(PartitionedRegionHelper.BUCKET_2_NODE_TABLE_PREFIX
  // + prid);
  // }
  // catch (IOException ieo) {
  // fail("IOException creating bucket2node",ieo);
  // } catch (ClassNotFoundException cne) {
  // fail("ClassNotFoundExcpetion creating bucket2node ", cne);
  // }
  // return ret;
  // }

  /**
   * This private methods sets the passed attributes and returns RegionAttribute object, which is
   * used in create region
   * 
   * @param redundancy
   * @param localMaxMem
   * 
   * @return
   */
  protected RegionAttributes createRegionAttributesForPR(int redundancy, int localMaxMem) {
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(redundancy).setLocalMaxMemory(localMaxMem)
        .setTotalNumBuckets(totalNumBuckets).create();
    attr.setPartitionAttributes(prAttr);
    return attr.create();
  }
}
