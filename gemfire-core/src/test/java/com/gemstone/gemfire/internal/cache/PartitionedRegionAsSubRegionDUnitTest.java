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

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * This dunit test verifies that PartitionedRegion destroyRegion, localDestroyRegion and close call works
 * properly when PartitionedRegion is in
 * ParentRegion/ChildRegion/PartitionedRegion hierarchy.
 * 
 * @author Tushar Apshankar, Created on May 19, 2006
 *  
 */
public class PartitionedRegionAsSubRegionDUnitTest extends
    PartitionedRegionDUnitTestCase
{

  //////constructor //////////
  public PartitionedRegionAsSubRegionDUnitTest(String name) {
    super(name);
  }//end of constructor

  public static final String PR_PREFIX = "PR";

  final String parentRegionName = "PARENT_REGION";

  final String childRegionName = "CHILD_REGION";

  /**
   * This creates PartitionedRegion as sub region of Distributed Region in
   * ParentRegion/ChildRegion/PartitionedRegion hierarchy
   */

  private CacheSerializableRunnable createPR = new CacheSerializableRunnable(
      "createPR") {
    public void run2() throws CacheException
    {
      Cache cache = getCache();
      Region parentRegion = cache.getRegion(Region.SEPARATOR + parentRegionName
          + Region.SEPARATOR + childRegionName);
      parentRegion.createSubregion(PR_PREFIX, createRegionAttributesForPR(
          1, 200));
    }
  };

  /**
   * This does put operations on the PartitionedRegion which is in
   * ParentRegion/ChildRegion/PartitionedRegion hierarchy
   */
  private CacheSerializableRunnable doRegionOps = new CacheSerializableRunnable(
      "doRegionOps") {
    public void run2() throws CacheException
    {
      Cache cache = getCache();
      PartitionedRegion pr = (PartitionedRegion)cache
          .getRegion(Region.SEPARATOR + parentRegionName + Region.SEPARATOR
              + childRegionName + Region.SEPARATOR + PR_PREFIX);
      for (int i = 0; i < pr.getTotalNumberOfBuckets(); i++) { 
        // Assume creating total number of bucket keys creates the maximum amount of buckets
        pr.put(new Integer(i), i + ""); 
      }

    }
  };

  /**
   * This creates child region as sub region of a parent region and then creates
   * PartitionedRegion as sub region of the child region in
   * ParentRegion/ChildRegion/PartitionedRegion hierarchy.
   */
  private CacheSerializableRunnable recreatePRAfterDestroy = new CacheSerializableRunnable(
      "recreatePRAfterDestroy") {
    public void run2() throws CacheException
    {
      Cache cache = getCache();
      Region parentRegion = cache
          .getRegion(Region.SEPARATOR + parentRegionName);
      Region childRegion = null;
      childRegion = parentRegion.createSubregion(childRegionName, parentRegion
          .getAttributes());
      PartitionedRegion pr = null;
      pr = (PartitionedRegion)cache.getRegion(Region.SEPARATOR
          + parentRegionName + Region.SEPARATOR + childRegionName
          + Region.SEPARATOR + PR_PREFIX);
      if (pr != null)
        fail("PR strill exists");

      pr = (PartitionedRegion)childRegion.createSubregion(PR_PREFIX,
          createRegionAttributesForPR(1, 200));
      //      Assert.assertTrue(pr.getBucket2Node().size()==0, "B2N cleanup was not
      // done");

      assertEquals(0, pr.getRegionAdvisor().getCreatedBucketsCount());
    }
  };

  /**
   * This creates child region as sub region of a parent region and then creates
   * PartitionedRegion as sub region of the child region in
   * ParentRegion/ChildRegion/PartitionedRegion hierarchy.
   */
  private CacheSerializableRunnable recreatePRAfterLocalDestroy = new CacheSerializableRunnable(
      "recreatePRAfterLocalDestroy") {
    public void run2() throws CacheException
    {
      Cache cache = getCache();
      Region parentRegion = cache
          .getRegion(Region.SEPARATOR + parentRegionName);
      Region childRegion = null;
      childRegion = parentRegion.createSubregion(childRegionName, parentRegion
          .getAttributes());
      PartitionedRegion pr = null;
      pr = (PartitionedRegion)cache.getRegion(Region.SEPARATOR
          + parentRegionName + Region.SEPARATOR + childRegionName
          + Region.SEPARATOR + PR_PREFIX);
      if (pr != null)
        fail("PR strill exists");

      pr = (PartitionedRegion)childRegion.createSubregion(PR_PREFIX,
          createRegionAttributesForPR(1, 200));
      //      Assert.assertTrue(pr.getBucket2Node().size()==0, "B2N cleanup was not
      // done");

      assertEquals(pr.getRegionAdvisor().getBucketSet().size(), 113);
    }
  };

  
  /**
   * This destroys child region from the
   * ParentRegion/ChildRegion/PartitionedRegion hierarchy
   */
  private CacheSerializableRunnable destroyChildRegion = new CacheSerializableRunnable(
      "destroyChildRegion") {
    public void run2() throws CacheException
    {
      Cache cache = getCache();
      Region parentRegion = cache.getRegion(Region.SEPARATOR + parentRegionName
          + Region.SEPARATOR + childRegionName);
      parentRegion.destroyRegion();
    }
  };

  /**
   * This destroys the PartitionedRegion from the
   * ParentRegion/ChildRegion/PartitionedRegion hierarchy
   */
  private CacheSerializableRunnable destroyPR = new CacheSerializableRunnable(
      "destroyPR") {
    public void run2() throws CacheException
    {
      Cache cache = getCache();
      Region pr = cache.getRegion(Region.SEPARATOR + PR_PREFIX);
      pr.destroyRegion();
    }
  };

  private CacheSerializableRunnable localDestroyChildRegion = new CacheSerializableRunnable(
      "localDestroyChildRegion") {
    public void run2() throws CacheException
    {
      Cache cache = getCache();
      Region parentRegion = cache.getRegion(Region.SEPARATOR + parentRegionName
          + Region.SEPARATOR + childRegionName);
      parentRegion.localDestroyRegion();
    }
  };

  private CacheSerializableRunnable closeChildRegion = new CacheSerializableRunnable(
      "closeChildRegion") {
    public void run2() throws CacheException
    {
      Cache cache = getCache();
      Region parentRegion = cache.getRegion(Region.SEPARATOR + parentRegionName
          + Region.SEPARATOR + childRegionName);
      parentRegion.close();
    }
  };

  /**
   * This class creates Parent and Child regions in
   * ParentRegion/ChildRegion/PartitionedRegion hierarchy
   */
  private CacheSerializableRunnable createDACKRegions = new CacheSerializableRunnable(
      "createDACKRegions") {
    public void run2() throws CacheException
    {
      Cache cache = getCache();
      RegionAttributes ra = createRegionAttributesForDACKRegions();
      Region parentRegion = cache.createRegion(parentRegionName, ra);
      parentRegion.createSubregion(childRegionName, ra);
    }
  };

  /**
   * This method test the destroyRegion call on a Child region in
   * ParentRegion/ChildRegion/PartitionedRegion hierarchy which has
   * PartitionedRegion as sub region.
   * 
   * @throws Exception
   */
  public void testSubRegionDestroyRegion() throws Exception
  {

    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(createDACKRegions);
    vm1.invoke(createDACKRegions);

    vm0.invoke(createPR);
    vm1.invoke(createPR);

    vm0.invoke(doRegionOps);

    vm0.invoke(destroyChildRegion);

    vm0.invoke(recreatePRAfterDestroy);

    vm0.invoke(destroyChildRegion);

    vm0.invoke(recreatePRAfterDestroy);
  }

  /**
   * This method test the localDestroyRegion call on a Child region in
   * ParentRegion/ChildRegion/PartitionedRegion hierarchy which has
   * PartitionedRegion as sub region.
   * 
   * @throws Exception
   */
  public void testSubRegionLocalDestroyRegion() throws Exception
  {

    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(createDACKRegions);
    vm1.invoke(createDACKRegions);

    vm0.invoke(createPR);
    vm1.invoke(createPR);

    vm0.invoke(doRegionOps);

    vm0.invoke(localDestroyChildRegion);

    vm0.invoke(recreatePRAfterLocalDestroy);
    vm0.invoke(localDestroyChildRegion);
  }
  
  /**
   * This method test the close call on a Child region in
   * ParentRegion/ChildRegion/PartitionedRegion hierarchy which has
   * PartitionedRegion as sub region.
   * 
   * @throws Exception
   */
  public void testSubRegionClose() throws Exception
  {

    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    vm0.invoke(createDACKRegions);
    vm1.invoke(createDACKRegions);

    vm0.invoke(createPR);
    vm1.invoke(createPR);

    vm0.invoke(doRegionOps);

    vm0.invoke(closeChildRegion);

    vm0.invoke(recreatePRAfterLocalDestroy);
    vm0.invoke(closeChildRegion);
  }

  /**
   * This private methods sets the passed attributes and returns RegionAttribute
   * object, which is used in create region
   * @param redundancy
   * @param localMaxMem
   * 
   * @return
   */
  protected RegionAttributes createRegionAttributesForPR(int redundancy,
      int localMaxMem) {
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.setRedundantCopies(redundancy)
        .setLocalMaxMemory(localMaxMem).create();
    attr.setPartitionAttributes(prAttr);
    return attr.create();
  }

  /**
   * This method creates RegionAttributes for the Parent and Child region of the
   * ParentRegion/ChildRegion/PartitionedRegion hierarchy
   * 
   * @return
   */
  protected RegionAttributes createRegionAttributesForDACKRegions() {
    AttributesFactory attr = new AttributesFactory();
    return attr.create();
  }
}
