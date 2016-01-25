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
/*
 * ClearGlobalDUnitTest.java
 * 
 * Created on September 13, 2005, 1:47 PM
 */

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.CacheObserverAdapter;
import com.gemstone.gemfire.internal.cache.CacheObserverHolder;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

import java.util.Properties;

/**
 * For the Global region subsequent puts should be blocked until the clear
 * operation is completely done
 *
 */
public class ClearGlobalDUnitTest extends DistributedTestCase
{

  /** Creates a new instance of ClearGlobalDUnitTest */
  public ClearGlobalDUnitTest(String name) {
    super(name);
  }
  static VM server1 = null;
  
  static Cache cache;

  static Properties props = new Properties();  

  static DistributedSystem ds = null;

  static Region region;  
  
  static boolean testFailed  = true;
  
  static StringBuffer exceptionMsg = new StringBuffer();
  
  static boolean testComplete = false;
  
  static Object lock = new Object();
  
  private static CacheObserver origObserver;
  
  /** name of the test region */
  private static final String REGION_NAME = "ClearGlobalDUnitTest_Region";

  public void setUp()throws Exception  {
    super.setUp();
    Host host = Host.getHost(0);
    server1 = host.getVM(0);    
    server1.invoke(ClearGlobalDUnitTest.class, "createCacheServer1");
    createCacheServer2();
    getLogWriter().fine("Cache created in successfully");
  }

  public void tearDown2()
  {        
    server1.invoke(ClearGlobalDUnitTest.class, "closeCache");
    resetClearCallBack();
    closeCache();
  }
  
  public static void resetClearCallBack()
  {
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER=false;
    CacheObserverHolder.setInstance(origObserver);
  }

   public static void closeCache() 
   {
     if (cache != null && !cache.isClosed()) {
       cache.close();
       cache.getDistributedSystem().disconnect();
     }  
   }

  public void testClearGlobalMultiVM() throws Exception
  {
    Object[] objArr = new Object[1];
    for (int i = 1; i < 4; i++) {
      objArr[0] = "" + i;
      server1.invoke(ClearGlobalDUnitTest.class, "putMethod", objArr);
    }    
    server1.invoke(ClearGlobalDUnitTest.class,"clearMethod");
    checkTestResults();
    
  }// end of test case

   public static void createCacheServer1() throws Exception
   {      
      ds = (new ClearGlobalDUnitTest("temp")).getSystem(props);
      cache = CacheFactory.create(ds);
      AttributesFactory factory  = new AttributesFactory();
      factory.setScope(Scope.GLOBAL);
      RegionAttributes attr = factory.create();      
      region = cache.createRegion(REGION_NAME, attr);          
     
    } //end of create cache for VM0

  public static void createCacheServer2() throws Exception
  { 
    ds = (new ClearGlobalDUnitTest("temp")).getSystem(props);
    CacheObserverImpl observer = new CacheObserverImpl();
    origObserver = CacheObserverHolder.setInstance(observer);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER=true;
    
    cache = CacheFactory.create(ds);      
    AttributesFactory factory  = new AttributesFactory();
    factory.setScope(Scope.GLOBAL);
    RegionAttributes attr = factory.create();      
    region = cache.createRegion(REGION_NAME, attr);
    cache.setLockTimeout(3);

  } //end of create cache for VM1
    
  public static void putMethod(Object ob) throws Exception
  {    
    if (ob != null) {
      String str = "first";
      region.put(ob, str);
    }   
  }

  public static void clearMethod() throws Exception 
  {
    region.clear();
  }
  public static void checkTestResults() throws Exception
  { 
    synchronized (lock) {      
      while (!testComplete) {
        try {
          lock.wait(5000);
        }
        catch (InterruptedException ex) {           
          fail("interrupted");
        }            
      }
    }
    
    if (testFailed){
      throw new Exception("Test Failed: " + exceptionMsg);
    }   
    else{  
      getLogWriter().info("Test Passed Successfully ");
    } 
  }
  
  public static class CacheObserverImpl extends CacheObserverAdapter
  {
    public void afterRegionClear(RegionEvent event)
    {
      Thread th = new PutThread();
      th.start();
      DistributedTestCase.join(th, 5 * 60 * 1000, getLogWriter());
      synchronized (lock) {    
        testComplete = true;
        lock.notify();
      }
      
    }   
  }//end of CacheObserverImpl

  static class PutThread extends Thread
  {
    public void run()
    {
      try {
        region.put("ClearGlobal", "PutThread");
        exceptionMsg.append(" Put operation should wait for clear operation to complete ");        
      }
      catch (TimeoutException ex) {
        //pass
        testFailed = false;        
        getLogWriter().info("Expected TimeoutException in thread ");        
      }
      catch (Exception ex) {        
        exceptionMsg.append(" Exception occurred while region.put(key,value)");                   
      }
    }
  }//end of PutThread
}// End of class
