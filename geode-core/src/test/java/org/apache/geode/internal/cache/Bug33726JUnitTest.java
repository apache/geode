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
package org.apache.geode.internal.cache;

import org.apache.geode.cache.*;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.fail;

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
    props.put(MCAST_PORT, "0");
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
