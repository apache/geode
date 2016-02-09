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
 * PutAllCallBkSingleVMDUnitTest.java
 *
 * Created on August 31, 2005, 4:17 PM
 */
package com.gemstone.gemfire.cache30;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;

public class PutAllCallBkSingleVMDUnitTest extends DistributedTestCase{
    
    /** Creates a new instance of PutAllCallBkSingleVMDUnitTest */
    public PutAllCallBkSingleVMDUnitTest(String name) {
        super(name);
    }
    
    static volatile Cache cache;
    static Properties props = new Properties();
    static Properties propsWork = new Properties();
    static volatile DistributedSystem ds = null;
    static volatile Region region;
    static boolean afterCreate=false;
    static boolean afterUpdate=false;
    static int putAllcounter = 0;
    static int afterUpdateputAllcounter = 0;
    static boolean beforeCreate=false;
    static boolean beforeUpdate=false;
    static int beforeCreateputAllcounter = 0;
    static int beforeUpdateputAllcounter = 0;
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(PutAllCallBkSingleVMDUnitTest.class, "createCache");
      vm1.invoke(PutAllCallBkSingleVMDUnitTest.class, "createCache");
      LogWriterUtils.getLogWriter().fine("Cache created in successfully");
    }
    
    @Override
    protected final void preTearDown() throws Exception {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(PutAllCallBkSingleVMDUnitTest.class, "closeCache");
      vm1.invoke(PutAllCallBkSingleVMDUnitTest.class, "closeCache");
    }
    
    public static synchronized void createCache(){
        try{
            CacheListener aListener = new AfterCreateCallback();
            CacheWriter aWriter = new BeforeCreateCallback();            
            ds = (new PutAllCallBkSingleVMDUnitTest("temp")).getSystem(props);
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setCacheWriter(aWriter);
            factory.setCacheListener(aListener);
            RegionAttributes attr = factory.create();
            
            region = cache.createRegion("map", attr);
            
            
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
    
    public static synchronized void closeCache(){
      region = null;
      if (cache != null) {
        cache.close();
        cache = null;
      }
      if (ds != null) {
        ds.disconnect();
        ds = null;
      }
    }

    //test methods
    public void testputAllSingleVM(){
        
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        
//        Object obj2;
        Object[] objArr = new Object[1];
        for (int i=0; i<5; i++){
            objArr[0] = ""+i;
            vm0.invoke(PutAllCallBkSingleVMDUnitTest.class, "putMethod", objArr);
            
        }
        
        vm0.invoke(PutAllCallBkSingleVMDUnitTest.class, "putAllMethod");
        
        vm0.invoke(new CacheSerializableRunnable("temp1"){
            public void run2() throws CacheException{
                if(!afterCreate){
                    fail("FAILED in aftercreate call back");
                }
                assertEquals(region.size(), putAllcounter); 
                assertEquals(region.size(), beforeCreateputAllcounter);
            }
        });
        
        vm0.invoke(new CacheSerializableRunnable("abc"){
            public void run2() throws CacheException{
                CacheListener bListener = new AfterUpdateCallback();  
                CacheWriter bWriter =new BeforeUpdateCallback();
                AttributesFactory factory  = new AttributesFactory();
                factory.setScope(Scope.DISTRIBUTED_ACK);
                factory.setCacheWriter(bWriter);
                factory.setCacheListener(bListener);
                RegionAttributes attr = factory.create();                
                Region tempRegion = cache.createRegion("temp", attr);                
                
                //to invoke afterUpdate we should make sure that entries are already present
                for(int i=0; i<5; i++){
                    tempRegion.put(new Integer(i), new String("region"+i));
                }
                
                Map m = new HashMap();
                for(int i=0; i<5; i++){
                    m.put(new Integer(i), new String("map"+i));
                }
                
                tempRegion.putAll(m, "putAllAfterUpdateCallback");
                
                //now, verifying callbacks
                if(!afterUpdate){
                    fail("FAILED in afterupdate call back");
                }
                assertEquals(tempRegion.size(), afterUpdateputAllcounter);
                assertEquals(tempRegion.size(), beforeUpdateputAllcounter); 
            }
        }
        );
        
    }//end of test case1
    
   
    public static Object putMethod(Object ob){
        Object obj=null;
        try{
            if(ob != null){
                String str = "first";
                obj = region.put(ob, str);
            }
        }catch(Exception ex){
            Assert.fail("Failed while region.put", ex);
        }
        return obj;
    }//end of putMethod
    
    public static void putAllMethod(){
        Map m = new HashMap();
        int i = 5, cntr = 0;
        try{
            while(cntr<21){
                m.put(new Integer(i), new String("map"+i));
                i++;
                cntr++;
            }
            
            region.putAll(m, "putAllCreateCallback");
            
        }catch(Exception ex){
            ex.printStackTrace();
            fail("Failed while region.putAll");
        }
    }//end of putAllMethod
    
    public static void putAllAfterUpdate(){
        Map m = new HashMap();
        int cntr = 0;
        try{
            for(int i=0; i<5; i++){
                m.put(""+i, new String("map_AfterUpdate"+i));
                cntr++;
            }
            region.putAll(m, "putAllAfterUpdateCallback");
        }catch(Exception ex){
            ex.printStackTrace();
            fail("Failed while region.putAll");
        }
    }//end of putAllAfterUpdate
    
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
    
    static class AfterCreateCallback extends CacheListenerAdapter {
        public void afterCreate(EntryEvent event){            
            putAllcounter++;
            LogWriterUtils.getLogWriter().fine("In afterCreate"+putAllcounter);
            if (event.getOperation().isPutAll()) {
              assertEquals("putAllCreateCallback", event.getCallbackArgument());
            }
            if(putAllcounter == 25){
                LogWriterUtils.getLogWriter().fine("performingtrue");
                afterCreate = true;
            }            
        }
    }
    
    static class AfterUpdateCallback extends CacheListenerAdapter {
        public void afterUpdate(EntryEvent event){            
            afterUpdateputAllcounter++;
            LogWriterUtils.getLogWriter().fine("In afterUpdate"+afterUpdateputAllcounter);
            if (event.getOperation().isPutAll()) {
              assertEquals("putAllAfterUpdateCallback", event.getCallbackArgument());
            }
            if(afterUpdateputAllcounter == 5){
                LogWriterUtils.getLogWriter().fine("performingtrue afterUpdate");
                afterUpdate = true;
            }            
        }
    }
    static class BeforeCreateCallback extends CacheWriterAdapter {
          public void beforeCreate(EntryEvent event){            
            beforeCreateputAllcounter++;
            LogWriterUtils.getLogWriter().fine("In beforeCreate"+beforeCreateputAllcounter);
            if (event.getOperation().isPutAll()) {
              assertEquals("putAllCreateCallback", event.getCallbackArgument());
            }
            if(beforeCreateputAllcounter == 25){
                LogWriterUtils.getLogWriter().fine("performingtrue beforeCreateputAll");
                beforeCreate = true;
            }            
        }
     }   
      static class BeforeUpdateCallback extends CacheWriterAdapter {
        public void beforeUpdate(EntryEvent event){            
            beforeUpdateputAllcounter++;
            LogWriterUtils.getLogWriter().fine("In beforeUpdate"+beforeUpdateputAllcounter);
            if (event.getOperation().isPutAll()) {
              assertEquals("putAllAfterUpdateCallback", event.getCallbackArgument());
            }
            if(beforeUpdateputAllcounter == 5){
                LogWriterUtils.getLogWriter().fine("performingtrue beforeUpdate");
                beforeUpdate = true;
            }            
        }
    }
    
}//end of class
