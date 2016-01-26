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
 * PutAllDAckDunitTest.java
 *
 * Created on September 15, 2005, 5:51 PM
 */
package com.gemstone.gemfire.internal.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 * @author vjadhav
 */
public class PutAllDAckDUnitTest extends DistributedTestCase {
    
    /** Creates a new instance of PutAllDAckDunitTest */
    public PutAllDAckDUnitTest(String name) {
        super(name);
    }
    static Cache cache;
    static Properties props = new Properties();
    static Properties propsWork = new Properties();
    static DistributedSystem ds = null;
    static Region region;
    static CacheTransactionManager cacheTxnMgr;
    static boolean beforeCreate=false;
    static int beforeCreateputAllcounter = 0;
    
    static boolean flag = false;
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(PutAllDAckDUnitTest.class, "createCacheForVM0");
      vm1.invoke(PutAllDAckDUnitTest.class, "createCacheForVM1");
      getLogWriter().fine("Cache created successfully");
    }
    
    public void tearDown2(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        vm0.invoke(PutAllDAckDUnitTest.class, "closeCache");
        vm1.invoke(PutAllDAckDUnitTest.class, "closeCache");
    }
    
    public static void createCacheForVM0() throws Exception {
            ds = (new PutAllDAckDUnitTest("temp")).getSystem(props);
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            RegionAttributes attr = factory.create();
            region = cache.createRegion("map", attr);
    }
    
    public static void createCacheForVM1() throws Exception {
            CacheWriter aWriter = new BeforeCreateCallback();
            ds = (new PutAllDAckDUnitTest("temp")).getSystem(props);
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setCacheWriter(aWriter);
            RegionAttributes attr = factory.create();
            region = cache.createRegion("map", attr);
    }
    public static void closeCache() throws Exception {
            //getLogWriter().fine("closing cache cache cache cache cache 33333333");
            cache.close();
            ds.disconnect();
            //getLogWriter().fine("closed cache cache cache cache cache 44444444");
    }
    
    //test methods
 
    public void testputAllRemoteVM(){
        // Test PASS. 
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        Object[] objArr = new Object[1];
        for (int i=0; i<2; i++){
            objArr[0] = ""+i;
            vm0.invoke(PutAllDAckDUnitTest.class, "putMethod", objArr);
        }
        vm0.invoke(PutAllDAckDUnitTest.class, "putAllMethod");
        flag = vm1.invokeBoolean(PutAllDAckDUnitTest.class,"getFlagVM1");
        
        vm1.invoke(new CacheSerializableRunnable("temp1"){
            public void run2() throws CacheException{
                if (flag){
                    
                    assertEquals(region.size(), beforeCreateputAllcounter);
                }
            }
        });
        
    }//end of test case1
    
    
    public static Object putMethod(Object ob){
        Object obj=null;
        try{
            if(ob != null){
                String str = "first";
                obj = region.put(ob, str);
            }
        }catch(Exception ex){
            fail("Failed while region.put", ex);
        }
        return obj;
    }//end of putMethod
    
    public static void putAllMethod(){
        Map m = new HashMap();
        int i = 2, cntr = 0;
        try{
            while(cntr<2){
                m.put(new Integer(i), new String("map"+i));
                i++;
                cntr++;
            }
            
            region.putAll(m);
            
        }catch(Exception ex){
            fail("Failed while region.putAll", ex);
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
    
    static class BeforeCreateCallback extends CacheWriterAdapter {
        public void beforeCreate(EntryEvent event){
//             try{
//                 Thread.sleep(20000);
//             }catch(InterruptedException ex) {
//                 //
//             }
            
            beforeCreateputAllcounter++;
            getLogWriter().fine("*******BeforeCreate*****");
            beforeCreate = true;
        }
    }
    public static boolean getFlagVM1(){
        return beforeCreate;
    }
    
}// end of class
