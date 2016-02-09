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
 * ClearMultiVmCallBkDUnitTest.java
 *
 * Created on August 11, 2005, 7:37 PM
 */
package com.gemstone.gemfire.cache30;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 * @author  prafulla/vjadhav
 */
public class ClearMultiVmCallBkDUnitTest extends DistributedTestCase{
    
    /** Creates a new instance of ClearMultiVmCallBkDUnitTest */
    public ClearMultiVmCallBkDUnitTest(String name) {
        super(name);
    }
    
    static Cache cache;
    static Properties props = new Properties();
    static Properties propsWork = new Properties();
    static DistributedSystem ds = null;
    static Region region;
    static Region paperWork;
    static CacheTransactionManager cacheTxnMgr;
    static boolean afterClear=false;
    
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(ClearMultiVmCallBkDUnitTest.class, "createCache");
      vm1.invoke(ClearMultiVmCallBkDUnitTest.class, "createCache");
      LogWriterUtils.getLogWriter().fine("Cache created in successfully");
    }
    
    @Override
    protected final void preTearDown() throws Exception {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(ClearMultiVmCallBkDUnitTest.class, "closeCache");
      vm1.invoke(ClearMultiVmCallBkDUnitTest.class, "closeCache");
    }
    
    public static void createCache(){
        try{
            CacheListener aListener = new ListenerCallBk();
//            props.setProperty("mcast-port", "1234");
//            ds = DistributedSystem.connect(props);
            ds = (new ClearMultiVmCallBkDUnitTest("temp")).getSystem(props);            
            
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            
            // Set Cachelisterner : aListener
            
            factory.setCacheListener(aListener);
            RegionAttributes attr = factory.create();
            
            region = cache.createRegion("map", attr);
            
            
        } catch (Exception ex){
            ex.printStackTrace();
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
    
    public void testClearSingleVM(){
        
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        //VM vm1 = host.getVM(1);
        
//        Object obj0;
//        Object obj1;
        Object[] objArr = new Object[1];
        for (int i=1; i<4; i++){
            objArr[0] = ""+i;
            vm0.invoke(ClearMultiVmCallBkDUnitTest.class, "putMethod", objArr);
            
        }
        LogWriterUtils.getLogWriter().fine("Did all puts successfully");
        
        vm0.invoke(ClearMultiVmCallBkDUnitTest.class,"clearMethod");
        LogWriterUtils.getLogWriter().fine("Did clear successfully");
        
        while(afterClear){
        }       
        
        int Regsize = vm0.invokeInt(ClearMultiVmCallBkDUnitTest.class, "sizeMethod");
        assertEquals(1, Regsize);
        
        
    }//end of test case1
    
     public void testClearMultiVM(){
        
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        Object[] objArr = new Object[1];
        for (int i=1; i<4; i++){
            objArr[0] = ""+i;
            vm0.invoke(ClearMultiVmCallBkDUnitTest.class, "putMethod", objArr);
            vm1.invoke(ClearMultiVmCallBkDUnitTest.class, "getMethod", objArr);
        }
        LogWriterUtils.getLogWriter().fine("Did all puts successfully");
        //vm0.invoke(ClearMultiVmCallBkDUnitTest.class,"putMethod");
        vm1.invoke(ClearMultiVmCallBkDUnitTest.class,"clearMethod");
        LogWriterUtils.getLogWriter().fine("Did clear successfully");
        
        while(afterClear){
        }       
        
        int Regsize = vm0.invokeInt(ClearMultiVmCallBkDUnitTest.class, "sizeMethod");
        assertEquals(1, Regsize);
        
        
    }//end of test case2
    
    public static Object putMethod(Object ob){
        Object obj=null;
        try{
            if(ob != null){
                String str = "first";
                obj = region.put(ob, str);
            }
        }catch(Exception ex){
            ex.printStackTrace();
            fail("Failed while region.put");
        }
        return obj;
    }
    
    public static Object getMethod(Object ob){
        Object obj=null;
        try{
            obj = region.get(ob);
        } catch(Exception ex){
            fail("Failed while region.get");
        }
        return obj;
    }
    
    public static boolean containsValueMethod(Object ob){
        boolean flag = false;
        try{
            flag = region.containsValue(ob);
        }catch(Exception ex){
            fail("Failed while region.containsValueMethod");
        }
        return flag;
    }
    
    public static int sizeMethod(){
        int i=0;
        try{
            i = region.size();
        }catch(Exception ex){
            fail("Failed while region.size");
        }
        return i;
    }
    
    public static void clearMethod(){
        try{
            region.clear();
        } catch(Exception ex){
            ex.printStackTrace();
        }
    }
    
    public static boolean getBoolean(){
        return afterClear;
    }
    
    static class ListenerCallBk extends CacheListenerAdapter {
  
        public void afterRegionClear(RegionEvent event){
            LogWriterUtils.getLogWriter().fine("In afterClear:: CacheListener Callback");
            try {
                int i = 7;
                region.put(""+i, "inAfterClear");
                afterClear = true;
            }catch (Exception e){
                //
            }
            
        }
        
    }
    
    
    
}//end of class
