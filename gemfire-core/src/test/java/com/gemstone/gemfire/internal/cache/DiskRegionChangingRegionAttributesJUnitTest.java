/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * This test will test that there are no unexpected behaviours
 * if the the region attributes are changed after starting it again.
 * 
 * The behaviour should be predictable
 * 
 * @author Mitul Bid
 *
 */
@Category(IntegrationTest.class)
public class DiskRegionChangingRegionAttributesJUnitTest extends
    DiskRegionTestingBase
{

  @Before
  public void setUp() throws Exception
  {
    super.setUp();
    props = new DiskRegionProperties();
    props.setDiskDirs(dirs);
    
  }

  @After
  public void tearDown() throws Exception
  {
    super.tearDown();
  }
  

  private DiskRegionProperties props;
  
  private void createOverflowOnly(){
    props.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache,props);
  }
  
  private void createPersistOnly(){
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,props, Scope.LOCAL);
  }
  
  private void createPersistAndOverflow(){
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,props); 
  }
  
  @Test
  public void testOverflowOnlyAndThenPersistOnly(){
    createOverflowOnly();
    put100Int();
    region.close();
    createPersistOnly();
    assertTrue(region.size()==0);
  }
  
  @Test
  public void testPersistOnlyAndThenOverflowOnly(){
    createPersistOnly();
    put100Int();
    region.close();
    try {
      createOverflowOnly();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    //Asif Recreate the region so that it gets destroyed in teardown 
    //clearing up the old Oplogs
    createPersistOnly();
    
  }
  
  @Test
  public void testOverflowOnlyAndThenPeristAndOverflow(){
    createOverflowOnly();
    put100Int();
    region.close();
    createPersistAndOverflow();
    assertTrue(region.size()==0);
  }
  
  @Test
  public void testPersistAndOverflowAndThenOverflowOnly(){
    createPersistAndOverflow();
    put100Int();
    region.close();
    try {
      createOverflowOnly();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    createPersistAndOverflow();
  }
  
 @Test
  public void testPersistOnlyAndThenPeristAndOverflow(){
   createPersistOnly();
   put100Int();
   region.close();
   createPersistAndOverflow();
   assertTrue(region.size()==100);
  }
  
  @Test
  public void testPersistAndOverflowAndThenPersistOnly(){
    createPersistAndOverflow();
    put100Int();
    region.close();
    createPersistOnly();
    assertTrue(region.size()==100);
  }
  
  
  
}

