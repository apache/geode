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

import java.util.Iterator;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This test aims to test the destroyRegion functionality.
 * 
 * @author rreja, Created on Jan 18, 2006
 * 
 *  
 */
public class PartitionedRegionDestroyDUnitTest extends
    PartitionedRegionDUnitTestCase
{

  //////constructor //////////
  public PartitionedRegionDestroyDUnitTest(String name) {

    super(name);
  }//end of constructor

  public static final String PR_PREFIX = "PR";

  final static int MAX_REGIONS = 2;
  
  final int totalNumBuckets = 5;
  
  VM vm0, vm1,vm2,vm3;
  
  public void testDestroyRegion() throws Exception, Throwable
  {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    AsyncInvocation async1 = null;
    CacheSerializableRunnable createPRs = new CacheSerializableRunnable(
        "createPrRegions") {

      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          cache.createRegion(PR_PREFIX + i,
              createRegionAttrsForPR(0, 200));
        }
        getLogWriter().info(
            "Successfully created " + MAX_REGIONS + " PartitionedRegions.");
      }
    };
    // Create PRs
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);
    vm2.invoke(createPRs);
    vm3.invoke(createPRs);

    vm1.invoke(new CacheSerializableRunnable("doPutOperations-1") {

      public void run2()
      {
        int j = 0;
        final String expectedExistsException = RegionDestroyedException.class.getName(); 
        getCache().getLogger().info("<ExpectedException action=add>" + 
     	expectedExistsException + "</ExpectedException>");
        try {
          Cache cache = getCache();
          for (; j < MAX_REGIONS; j++) {
            PartitionedRegion pr = (PartitionedRegion) cache.getRegion(Region.SEPARATOR + PR_PREFIX + j);
            assertNotNull(pr);
            // Create enough entries such that all bucket are created, integer keys assumes mod distribution
            int totalEntries = pr.getTotalNumberOfBuckets() * 2;
            for (int k = 0; k < totalEntries; k++) {
              pr.put(new Integer(k), PR_PREFIX + k);
            }
          }
          
        }
        catch (RegionDestroyedException e) {
         // getLogWriter().info (
              //"RegionDestroyedException occured for Region = " + PR_PREFIX + j);
        }
        getCache().getLogger().info("<ExpectedException action=remove>" + 
        expectedExistsException + "</ExpectedException>");
      }
    });
    async1 = vm2.invokeAsync(new CacheSerializableRunnable("doPutOperations-2") {

      public void run2() throws CacheException
      {

        int j = 0;
        final String expectedException = RegionDestroyedException.class.getName(); 
        getCache().getLogger().info("<ExpectedException action=add>" + 
        expectedException + "</ExpectedException>"); 	
        try {
          Cache cache = getCache();
          
          // Grab the regions right away, before they get destroyed
          // by the other thread
          PartitionedRegion prs[] = new PartitionedRegion[MAX_REGIONS];
          for (j = 0; j < MAX_REGIONS; j ++) {
            prs[j] = (PartitionedRegion) cache.getRegion(
                Region.SEPARATOR + PR_PREFIX + j);
            if (prs[j] == null) {
              fail("Region was destroyed before putter could find it");
            }
          }
          
          for (j = 0; j < MAX_REGIONS; j++) {
            PartitionedRegion pr = prs[j];
            assertNotNull(pr);
            int startEntries = pr.getTotalNumberOfBuckets() * 20;
            int endEntries = startEntries + pr.getTotalNumberOfBuckets();
            for (int k = startEntries; k < endEntries; k++) {
              pr.put(new Integer(k), PR_PREFIX + k);
            }
            try {
              Thread.sleep(100);
            }
            catch (InterruptedException ie) {
              fail("interrupted");
            }
          }
        }
        catch (RegionDestroyedException e) {
          getLogWriter().info(
              "RegionDestroyedException occured for Region = " + PR_PREFIX + j);
        }
        getCache().getLogger().info("<ExpectedException action=remove>" + 
        		expectedException + "</ExpectedException>"); 
      }
    });

    DistributedTestCase.join(async1, 30 * 1000, getLogWriter());
    if(async1.exceptionOccurred()) {
      fail("async1 failed", async1.getException());
    }
    final String expectedExceptions = "com.gemstone.gemfire.distributed.internal.ReplyException"; 
    addExceptionTag(expectedExceptions);
    
    pause(1000); // give async a chance to grab the regions...
    
    vm0.invoke(new CacheSerializableRunnable("destroyPRRegions") {

      public void run2() throws CacheException
      {
        Cache cache = getCache();
        for (int i = 0; i < MAX_REGIONS; i++) {
          Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX + i);
          assertNotNull(pr);
          pr.destroyRegion();
          assertTrue(pr.isDestroyed());
          Region prDes = cache.getRegion(Region.SEPARATOR + PR_PREFIX + i);
          assertNull(prDes);
        }
      }
    });
    
    addExceptionTag(expectedExceptions);
    CacheSerializableRunnable validateMetaDataAfterDestroy = new CacheSerializableRunnable(
        "validateMetaDataAfterDestroy") {

      public void run2() throws CacheException
      {

        Cache cache = getCache();
        Region rootRegion = PartitionedRegionHelper.getPRRoot(cache);
//        Region allPRs = PartitionedRegionHelper.getPRConfigRegion(rootRegion,
//            getCache());

        int trial = 0;
        // verify that all the regions have received the destroy call.
        while (trial < 10) {
          if (cache.rootRegions().size() > 1) {
            try {
              Thread.sleep(500);
            }
            catch (InterruptedException e) {
              fail("interrupted");
            }
            trial++;
          }
          else {
            break;
          }
        }

        if (cache.rootRegions().size() > 1) {
          fail("All Regions Not destroyed. # OF Regions Not Destroyed = "
              + (cache.rootRegions().size() - 1));
        }

        // Assert that all PartitionedRegions are gone
        assertEquals(0, rootRegion.size());
        getLogWriter().info("allPartitionedRegions size() =" + rootRegion.size());
        assertEquals("ThePrIdToPR Map size is:"+PartitionedRegion.prIdToPR.size()+" instead of 0", MAX_REGIONS, PartitionedRegion.prIdToPR.size());
        getLogWriter().info(
            "PartitionedRegion.prIdToPR.size() ="
                + PartitionedRegion.prIdToPR.size());
        getLogWriter().info(
            "# of Subregions of root Region after destroy call = "
                + rootRegion.subregions(false).size());
        Iterator itr = (rootRegion.subregions(false)).iterator();
        while (itr.hasNext()) {
          Region rg = (Region)itr.next();
          getLogWriter().info("Root Region SubRegionName = " + rg.getName());
//          assertEquals("REGION NAME FOUND:"+rg.getName(),-1, rg.getName().indexOf(
//              PartitionedRegionHelper.BUCKET_2_NODE_TABLE_PREFIX));
          assertEquals("regionFound that should be gone!:"+rg.getName(),-1, rg.getName().indexOf(
              PartitionedRegionHelper.BUCKET_REGION_PREFIX));
        }
      }
    };
    vm0.invoke(validateMetaDataAfterDestroy);
    vm1.invoke(validateMetaDataAfterDestroy);
    vm2.invoke(validateMetaDataAfterDestroy);
    vm3.invoke(validateMetaDataAfterDestroy);
    vm0.invoke(createPRs);
    vm1.invoke(createPRs);
    vm2.invoke(createPRs);
    vm3.invoke(createPRs);
    
  }

  protected RegionAttributes createRegionAttrsForPR(int red, int localMaxMem)
  {

    AttributesFactory attr = new AttributesFactory();
    attr.setDataPolicy(DataPolicy.PARTITION);
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(red)
        .setLocalMaxMemory(localMaxMem)
        .setTotalNumBuckets(totalNumBuckets)
        .create();
    attr.setPartitionAttributes(prAttr);
    return attr.create();
  }
  
  private void addExceptionTag(final String expectedException)
  {
	
	  SerializableRunnable addExceptionTag = new CacheSerializableRunnable("addExceptionTag")
	  {
		 public void run2()
		 {
			 getCache().getLogger().info("<ExpectedException action=add>" + 
						expectedException + "</ExpectedException>"); 
		 }
	  };
	  
	 vm0.invoke(addExceptionTag);
	 vm1.invoke(addExceptionTag);
	 vm2.invoke(addExceptionTag);
	 vm3.invoke(addExceptionTag);
  }
 
  private void removeExceptionTag(final String expectedException)
  {	
	
	  SerializableRunnable removeExceptionTag = new CacheSerializableRunnable("removeExceptionTag")
	  {
		public void run2() throws CacheException
		{
			  getCache().getLogger().info("<ExpectedException action=remove>" + 
						 expectedException + "</ExpectedException>");	
		}
	  };
	  vm0.invoke(removeExceptionTag);
	  vm1.invoke(removeExceptionTag);
	  vm2.invoke(removeExceptionTag);
	  vm3.invoke(removeExceptionTag);	
  }
}
