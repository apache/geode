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
 * PutAllCallBkRemoteVMDUnitTest.java
 *
 * Created on September 2, 2005, 2:49 PM
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
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 * @author vjadhav
 */
public class PutAllCallBkRemoteVMDUnitTest extends DistributedTestCase {
    
    /** Creates a new instance of PutAllCallBkRemoteVMDUnitTest */
    public PutAllCallBkRemoteVMDUnitTest(String name) {
        super(name);
    }
    
    static volatile Cache cache;
    static Properties props = new Properties();
    static Properties propsWork = new Properties();
    static volatile DistributedSystem ds = null;
    static volatile Region region;
    static volatile Region paperRegion;
    static boolean afterCreate=false;
    static boolean afterUpdate=false;
    static int putAllcounter = 0;
    static int afterUpdateputAllcounter = 0;
    static boolean beforeCreate=false;
    static boolean beforeUpdate=false;
    static int forCreate = 0;
    static int forUpdate = 0;
    static int beforeCreateputAllcounter = 0;
    static int beforeUpdateputAllcounter = 0;
    static boolean notified = false;
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(PutAllCallBkRemoteVMDUnitTest.class, "createCacheForVM0");
      vm1.invoke(PutAllCallBkRemoteVMDUnitTest.class, "createCacheForVM1");
      getLogWriter().info("Cache created successfully");
    }
    
    public void tearDown2(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        vm0.invoke(PutAllCallBkRemoteVMDUnitTest.class, "closeCache");
        vm1.invoke(PutAllCallBkRemoteVMDUnitTest.class, "closeCache");
    }
    
    public static synchronized void createCacheForVM0(){
        try{
            ds = (new PutAllCallBkRemoteVMDUnitTest("temp")).getSystem(props);
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
            factory.setDataPolicy(DataPolicy.REPLICATE);
            factory.setScope(Scope.DISTRIBUTED_ACK);
            RegionAttributes attr1 = factory.create();
            paperRegion = cache.createRegion("paper", attr1);
            RegionAttributes attr = factory.create();
            region = cache.createRegion("map", attr);
              
        } catch (CacheException ex){
            throw new RuntimeException("vm0 cache creation exception", ex);
        }
    }
    
    public static void createCacheForVM1(){
        try{
            CacheListener aListener = new AfterCreateCallback();
            CacheWriter aWriter = new BeforeCreateCallback();
            
            ds = (new PutAllCallBkRemoteVMDUnitTest("temp")).getSystem(props);
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
            factory.setDataPolicy(DataPolicy.REPLICATE);
            factory.setScope(Scope.DISTRIBUTED_ACK);
            RegionAttributes attr1 = factory.create();
            paperRegion = cache.createRegion("paper", attr1);
            factory.setCacheWriter(aWriter);
            factory.addCacheListener(aListener);
            RegionAttributes attr = factory.create();
            region = cache.createRegion("map", attr);
        } catch (CacheException ex){
            throw new RuntimeException("vm1 cache creation exception", ex);
        }
    }
    
    
    public static synchronized void closeCache(){
      paperRegion = null;
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
    
    public void testputAllRemoteVM(){
        
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        ////////////////testing create call backs//////////////
        
        vm0.invoke(new CacheSerializableRunnable("put entries"){
            public void run2() throws CacheException{
                Map m = new HashMap();
                paperRegion.put("callbackCame","false");
                try{
                    for(int i=1; i<21; i++){
                        m.put(new Integer(i), java.lang.Integer.toString(i));
                    }
                    region.putAll(m);
                    
                }catch (Exception ex){
                    throw new RuntimeException("exception putting entries", ex);
                }
                getLogWriter().info("****************paperRegion.get(afterCreate)***************"+paperRegion.get("afterCreate"));

                WaitCriterion ev = new WaitCriterion() {
                  public boolean done() {
                    int size = region.size();
                    if (size != ((Integer)paperRegion.get("afterCreate")).intValue()-1) {
                      return false;
                    }
                    if (size != ((Integer)paperRegion.get("beforeCreate")).intValue()-1) {
                      return false;
                    }
                    return true;
                  }
                  public String description() {
                    return "Waiting for event";
                  }
                };
                DistributedTestCase.waitForCriterion(ev, 3000, 200, true);
            }
        });
        
        
        vm1.invoke(new CacheSerializableRunnable("validate callbacks"){
            public void run2() throws CacheException{
                if(!notified){
                    try{
                        synchronized(PutAllCallBkRemoteVMDUnitTest.class){
                            this.wait();
                        }
                    } catch(Exception e){
                        
                    }
                }
                if(!paperRegion.get("callbackCame").equals("true")){
                    fail("Failed in aftercreate call back :: PutAllCallBkRemoteVMDUnitTest ");
                }
    
            }
        });
        
        
        //to test afterUpdate
        
    }
    
    public void testPutAllAfterUpdateCallbacks(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        vm0.invoke(new CacheSerializableRunnable("put and then update"){
            public void run2() throws CacheException{
                paperRegion.put("callbackCame","false");             
                //to invoke afterUpdate we should make sure that entries are already present
                for(int i=0; i<5; i++){
                    region.put(new Integer(i), new String("region"+i));
                }
                
                Map m = new HashMap();
                for(int i=0; i<5; i++){
                    m.put(new Integer(i), new String("map"+i));
                }
                
                region.putAll(m);
                
//                 try{
//                     Thread.sleep(3000);
//                 }catch(InterruptedException ex){
//                     //
//                 }
                
                assertEquals(region.size(), ((Integer)paperRegion.get("beforeUpdate")).intValue()-1);
                assertEquals(region.size(), ((Integer)paperRegion.get("afterUpdate")).intValue()-1);
            }
        }
        );
        
        vm1.invoke(new CacheSerializableRunnable("validate callbacks"){
            public void run2() throws CacheException{
                
                if(!notified){
                    try{
                        synchronized(PutAllCallBkRemoteVMDUnitTest.class){
                            this.wait();
                        }
                    } catch(Exception e){
                        
                    }
                }
                
                if(!paperRegion.get("callbackCame").equals("true")){
                    fail("Failed in afterUpdate call back :: PutAllCallBkRemoteVMDUnitTest");
                }
                
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
            ex.printStackTrace();
            fail("Failed while region.put");
        }
        return obj;
    }//end of putMethod
    
    public static void putAllMethod(){
        Map m = new HashMap();
        int i = 5, cntr = 0;
        try{
            while(cntr<20){
                m.put(new Integer(i), new String("map"+i));
                i++;
                cntr++;
            }
            
            region.putAll(m);
            
        }catch(Exception ex){
            ex.printStackTrace();
            fail("Failed while region.putAll");
        }
    }//end of putAllMethod
    
    
    public static Object getMethod(Object ob){
        Object obj=null;
        try{
            obj = region.get(ob);
        } catch(Exception ex){
            fail("Failed while region.get");
        }
        return obj;
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
            paperRegion.put("callbackCame","true");
            Integer counter = (Integer)paperRegion.get("afterCreate");
            if(counter==null) counter = new Integer(1);
            paperRegion.put("afterCreate",new Integer(counter.intValue()+1));
            
            getLogWriter().info("In afterCreate"+putAllcounter);
            if(putAllcounter == forCreate){
                getLogWriter().info("performingtrue");
                afterCreate = true;
            }
            try{
                synchronized(PutAllCallBkRemoteVMDUnitTest.class){
                    this.notify();
                }
            } catch(Exception e){
                
            }
            notified = true;
            getLogWriter().info("*******afterCreate***** Key :"+event.getKey()+ " Value :"+event.getNewValue());
        }
        
        public void afterUpdate(EntryEvent event){
            paperRegion.put("callbackCame","true");
            Integer counter = (Integer)paperRegion.get("afterUpdate");
            if(counter==null) counter = new Integer(1);
            paperRegion.put("afterUpdate",new Integer(counter.intValue()+1));
            getLogWriter().info("In afterUpdate"+afterUpdateputAllcounter);
            if(afterUpdateputAllcounter == forUpdate){
                getLogWriter().info("performingtrue afterUpdate");
                afterUpdate = true;
            }
            try{
                synchronized(PutAllCallBkRemoteVMDUnitTest.class){
                    this.notify();
                }
            } catch(Exception e){
                
            }
            
            notified = true;
            
            getLogWriter().info("*******afterUpdate***** Key :"+event.getKey()+ " Value :"+event.getNewValue());
            
        }
    }
    static class BeforeCreateCallback extends CacheWriterAdapter {
        //    static class BeforeCreateCallback extends CapacityControllerAdapter {
        public void beforeCreate(EntryEvent event){
            Integer counter = (Integer)paperRegion.get("beforeCreate");
            if(counter==null) counter = new Integer(1);
            paperRegion.put("beforeCreate",new Integer(counter.intValue()+1));
            getLogWriter().info("*******BeforeCreate***** event="+event);
        }
        
        public void beforeUpdate(EntryEvent event) {
            Integer counter = (Integer)paperRegion.get("beforeUpdate");
            if(counter==null) counter = new Integer(1);
            paperRegion.put("beforeUpdate",new Integer(counter.intValue()+1));
            getLogWriter().info("In beforeUpdate"+beforeUpdateputAllcounter);
            getLogWriter().info("*******BeforeUpdate***** event="+event);
        }
    }
}// end of test class
