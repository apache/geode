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

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

public class Bug33726DUnitTest extends DistributedTestCase {
  
  static boolean[] flags = new boolean[2];
  static Cache cache = null;
  static DistributedSystem ds = null;
  static boolean isOK = false;
  

  public Bug33726DUnitTest(String name){
	super(name);
  }

  public void tearDown2() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(Bug33726DUnitTest.class, "closeCache");
    vm1.invoke(Bug33726DUnitTest.class, "closeCache");
  }
  
  public static void closeCache(){
  try{
	cache.close();
	cache = null;
	ds.disconnect();
	}catch (Exception ex){
	ex.printStackTrace();
	}
   }

  public void testAfterRegionCreate() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    vm0.invoke(Bug33726DUnitTest.class, "createCacheAndPopulateRegion1");
    vm1.invoke(Bug33726DUnitTest.class, "createCacheAndRegion2");
    boolean pass = vm1.invokeBoolean(Bug33726DUnitTest.class, "testFlag");
    assertTrue("The test failed", pass);
  
  }
  
  public static void createCacheAndPopulateRegion1() {
    try {
      ds = (new Bug33726DUnitTest("temp")).getSystem(new Properties());
      cache = CacheFactory.create(ds);
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.GLOBAL);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      RegionAttributes attr = factory.create();
      Region region = cache.createRegion("testRegion", attr);
      Region subRegion = region.createSubregion("testSubRegion",attr);
      for(int i=1; i < 100; i++){
        region.put(new Integer(i), new Integer(i));      
        subRegion.put(new Integer(i), new Integer(i));
      }
     }
    catch (Exception ex) {
      fail("Creation of cache failed due to "+ex);
      ex.printStackTrace();
    }
  }

  public static void createCacheAndRegion2() {
    try {
      ds = (new Bug33726DUnitTest("temp")).getSystem(new Properties());
      cache = CacheFactory.create(ds);
      AttributesFactory factory = new AttributesFactory();
      factory.setCacheListener(new TestCacheListener());
      factory.setScope(Scope.GLOBAL);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      RegionAttributes attr = factory.create();
      Region region = cache.createRegion("testRegion", attr);
      region.createSubregion("testSubRegion",attr);
	  }
    catch (Exception ex) {
        fail("failed due to "+ex);
      ex.printStackTrace();
    }
  }
  
  public static boolean testFlag() {
    if (isOK) {
      return isOK;
    }
    else {
      synchronized (Bug33726DUnitTest.class) {
        if (isOK) {
          return isOK;
        }
        else {
          try {
            Bug33726DUnitTest.class.wait(120000);
          }
          catch (InterruptedException ie) {
            fail("interrupted");
          }
        }
      }
      return isOK;
    }
  }

  public static class TestCacheListener extends CacheListenerAdapter {

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
        synchronized(Bug33726DUnitTest.class){
        Bug33726DUnitTest.class.notify();
        }
      }
    }
  }
  
}
