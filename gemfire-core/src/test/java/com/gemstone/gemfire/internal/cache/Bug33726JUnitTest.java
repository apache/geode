/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * AFTER_REGION_CREATE was being sent before region
 * initialization (bug 33726). Test to verify that that is no longer the case.
 *
 */
@Category(IntegrationTest.class)
public class Bug33726JUnitTest{
  
  boolean[] flags = new boolean[2];
//  private boolean failed = false;
//  private boolean done = false;
  static boolean isOK = false;
  
  public Bug33726JUnitTest(){
    
  }
  
  public void setup(){
    
  }
  
  @After
  public void tearDown(){
    
  }
  
  
  
  @Test
  public void testAfterRegionCreate() {
    Properties props = new Properties();
    DistributedSystem ds = DistributedSystem.connect(props);
    AttributesFactory factory = new AttributesFactory();
    factory.setCacheListener(new TestCacheListener());
    Cache cache = null;
    try {
      cache = CacheFactory.create(ds);
     
      Region region = cache.createRegion("testRegion", factory.create());
      region.createSubregion("testSubRegion",factory.create());
    }
    catch (Exception e) {
      fail("Failed to create cache due to " + e);
      e.printStackTrace();
    }
    
   
    if(!testFlag()){
      fail("After create sent although region was not initialized");
    }
  }
  
  public  boolean testFlag() {
    if (isOK) {
      return isOK;
    }
    else {
      synchronized (Bug33726JUnitTest.class) {
        if (isOK) {
          return isOK;
        }
        else {
          try {
            Bug33726JUnitTest.class.wait(120000);
          }
          catch (InterruptedException ie) {
            fail("interrupted");
          }
        }
      }
      return isOK;
    }
  }
  
  protected class TestCacheListener extends CacheListenerAdapter {

    public void afterRegionCreate(RegionEvent event) {
      Region region = event.getRegion();
      if (((LocalRegion) region).isInitialized()) {
        String regionPath = event.getRegion().getFullPath();
        if (regionPath.indexOf("/testRegion/testSubRegion") >= 0) {
          flags[1] = true;
        }
        else if (regionPath.indexOf("/testRegion") >= 0) {
          flags[0] = true;
        }
      
      }
      if(flags[0] && flags[1]){
        isOK = true;
        synchronized (Bug33726JUnitTest.class) {
        Bug33726JUnitTest.class.notify();
        }
      }
    }
  }
}
