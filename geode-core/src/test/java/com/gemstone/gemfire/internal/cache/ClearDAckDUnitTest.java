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
 * ClearDAckDUnitTest.java
 *
 * Created on September 6, 2005, 2:57 PM
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 * @author vjadhav
 */
public class ClearDAckDUnitTest extends DistributedTestCase {
    
    /** Creates a new instance of ClearDAckDUnitTest */
    public ClearDAckDUnitTest(String name) {
        super(name);
    }
    
    static Cache cache;
    static Properties props = new Properties();
    static Properties propsWork = new Properties();
    static DistributedSystem ds = null;
    static Region region;
    static Region paperWork;
    static CacheTransactionManager cacheTxnMgr;
    static boolean IsAfterClear=false;
    static boolean flag = false;
    static DistributedMember vm0ID, vm1ID;
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0ID = (DistributedMember)vm0.invoke(ClearDAckDUnitTest.class, "createCacheVM0");
      vm1ID = (DistributedMember)vm1.invoke(ClearDAckDUnitTest.class, "createCacheVM1");
      LogWriterUtils.getLogWriter().info("Cache created in successfully");
    }
    
    @Override
    protected final void preTearDown() throws Exception {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      VM vm2 = host.getVM(2);
      vm0.invoke(ClearDAckDUnitTest.class, "closeCache");
      vm1.invoke(ClearDAckDUnitTest.class, "resetClearCallBack");
      vm1.invoke(ClearDAckDUnitTest.class, "closeCache");
      vm2.invoke(ClearDAckDUnitTest.class, "closeCache");
      cache = null;
      Invoke.invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
    }
    
    public static long getRegionVersion(DistributedMember memberID) {
      return ((LocalRegion)region).getVersionVector().getVersionForMember((VersionSource)memberID);
    }
    
  private static CacheObserver origObserver;
  public static void resetClearCallBack()
  {
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER=false;
    CacheObserverHolder.setInstance(origObserver);
  }

    public static DistributedMember createCacheVM0() {
        try{
            //            props.setProperty("mcast-port", "1234");
            //            ds = DistributedSystem.connect(props);
            LogWriterUtils.getLogWriter().info("I am vm0");
            ds = (new ClearDAckDUnitTest("temp")).getSystem(props);
            cache = CacheFactory.create(ds);
            
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.REPLICATE);
            //[bruce]introduces bad race condition if mcast is used, so
            //       the next line is disabled
            //factory.setEarlyAck(true);
            //DistributedSystem.setThreadsSocketPolicy(false);
            RegionAttributes attr = factory.create();
            
            region = cache.createRegion("map", attr);
            LogWriterUtils.getLogWriter().info("vm0 map region: " + region);
            paperWork = cache.createRegion("paperWork", attr);
            return cache.getDistributedSystem().getDistributedMember();
        } catch (CacheException ex){
            throw new RuntimeException("createCacheVM0 exception", ex);
        }
    } //end of create cache for VM0
    public static DistributedMember createCacheVM1(){
        try{
            //   props.setProperty("mcast-port", "1234");
            //   ds = DistributedSystem.connect(props);
            LogWriterUtils.getLogWriter().info("I am vm1");
            ds = (new ClearDAckDUnitTest("temp")).getSystem(props);
            //DistributedSystem.setThreadsSocketPolicy(false);
            CacheObserverImpl observer = new CacheObserverImpl();
            origObserver = CacheObserverHolder.setInstance(observer);
            LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER=true;
            
            cache = CacheFactory.create(ds);
            
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.REPLICATE);
            
            RegionAttributes attr = factory.create();
            
            region = cache.createRegion("map", attr);
            LogWriterUtils.getLogWriter().info("vm1 map region: " + region);
            paperWork = cache.createRegion("paperWork", attr);
            return cache.getDistributedSystem().getDistributedMember();
            
        } catch (CacheException ex){
            throw new RuntimeException("createCacheVM1 exception", ex);
        }
    }
    
    public static void createCacheVM2AndLocalClear(){
      try{
          //   props.setProperty("mcast-port", "1234");
          //   ds = DistributedSystem.connect(props);
          LogWriterUtils.getLogWriter().info("I am vm2");
          ds = (new ClearDAckDUnitTest("temp")).getSystem(props);
          //DistributedSystem.setThreadsSocketPolicy(false);
          CacheObserverImpl observer = new CacheObserverImpl();
          origObserver = CacheObserverHolder.setInstance(observer);
          LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER=true;
          
          cache = CacheFactory.create(ds);
          
          AttributesFactory factory  = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.NORMAL);
          
          RegionAttributes attr = factory.create();
          
          region = cache.createRegion("map", attr);
          LogWriterUtils.getLogWriter().info("vm2 map region: " + region);
          paperWork = cache.createRegion("paperWork", attr);
          
          region.put("vm2Key", "vm2Value");
          region.localClear();
          
      } catch (CacheException ex){
          throw new RuntimeException("createCacheVM1 exception", ex);
      }
  }
  
    public static void closeCache(){
        try{
            cache.close();
            ds.disconnect();
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
    
    //test methods
    
    
    public void testClearMultiVM(){
        
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        Object[] objArr = new Object[1];
        for (int i=1; i<4; i++){
            objArr[0] = ""+i;
            vm0.invoke(ClearDAckDUnitTest.class, "putMethod", objArr);
            
        }
        LogWriterUtils.getLogWriter().info("Did all puts successfully");
        
        long regionVersion = (Long)vm1.invoke(ClearDAckDUnitTest.class, "getRegionVersion", new Object[]{vm0ID});
        
        vm0.invoke(ClearDAckDUnitTest.class,"clearMethod");
        
        boolean flag = vm1.invokeBoolean(ClearDAckDUnitTest.class,"getVM1Flag");
        LogWriterUtils.getLogWriter().fine("Flag in VM1="+ flag);
        
        assertTrue(flag);
        
        long newRegionVersion = (Long)vm1.invoke(ClearDAckDUnitTest.class, "getRegionVersion", new Object[]{vm0ID});
        assertEquals("expected clear() to increment region version by 1 for " + vm0ID, regionVersion+1, newRegionVersion);
        
        // test that localClear does not distribute
        VM vm2 = host.getVM(2);
        vm2.invoke(ClearDAckDUnitTest.class, "createCacheVM2AndLocalClear");
        
        flag = vm1.invokeBoolean(ClearDAckDUnitTest.class,"getVM1Flag");
        LogWriterUtils.getLogWriter().fine("Flag in VM1="+ flag);
        assertFalse(flag);
        
    }//end of test case
    
    public static Object putMethod(Object ob){
        Object obj=null;
//        try{
            if(ob != null){
                String str = "first";
                obj = region.put(ob, str);
            }
//        }catch(Exception ex){
//            ex.printStackTrace();
//            fail("Failed while region.put");
//        }
        return obj;
    }
    
    
    // public static boolean clearMethod(){
    public static void clearMethod(){
        try{
        
            long start = System.currentTimeMillis();
            region.clear();
            long end = System.currentTimeMillis();
            
            long diff = end - start;
            LogWriterUtils.getLogWriter().info("Clear Thread proceeded before receiving the ack message in (milli seconds): "+diff);
              
        }catch (Exception e){
            e.printStackTrace();
        }
        
    }
    
    
    public static class CacheObserverImpl extends CacheObserverAdapter {
        
        public void afterRegionClear(RegionEvent event){
            IsAfterClear = true;
        }
        
    }
    
    public static boolean getVM1Flag() {
      boolean result = IsAfterClear;
      IsAfterClear = false;
      return result;
    }
    
    
    
}// end of test class
