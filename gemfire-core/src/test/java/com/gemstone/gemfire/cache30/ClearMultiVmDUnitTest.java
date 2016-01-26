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
 * ClearMultiVmDUnitTest.java
 *
 * Created on August 11, 2005, 7:37 PM
 */
package com.gemstone.gemfire.cache30;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 * @author  prafulla
 */
public class ClearMultiVmDUnitTest extends DistributedTestCase{
    
    /** Creates a new instance of ClearMultiVmDUnitTest */
    public ClearMultiVmDUnitTest(String name) {
        super(name);
    }
    
    static Cache cache;
    static Properties props = new Properties();
    static Properties propsWork = new Properties();
    static DistributedSystem ds = null;
    static Region region;
    static Region paperWork;
    static Region mirroredRegion;
    static CacheTransactionManager cacheTxnMgr;
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(ClearMultiVmDUnitTest.class, "createCache");
      vm1.invoke(ClearMultiVmDUnitTest.class, "createCache");
    }
    
    public void tearDown2(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        vm0.invoke(ClearMultiVmDUnitTest.class, "closeCache");
        vm1.invoke(ClearMultiVmDUnitTest.class, "closeCache");
        cache = null;
        invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
        
    }
    
    public static void createCache(){
        try{            
            ds = (new ClearMultiVmDUnitTest("temp")).getSystem(props);
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setConcurrencyChecksEnabled(true);
            RegionAttributes attr = factory.create();
            region = cache.createRegion("map", attr);
            
            AttributesFactory factory1  = new AttributesFactory();
            factory1.setScope(Scope.DISTRIBUTED_ACK);
            factory.setConcurrencyChecksEnabled(true);
            factory1.setDataPolicy(DataPolicy.REPLICATE);
            paperWork = cache.createRegion("paperWork", factory1.create());
            
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
    
    public void testClearSimpleScenarios(){
        
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        //verifying Single VM clear functionalities
        vm0.invoke(new CacheSerializableRunnable("temp1"){
            public void run2()throws CacheException{
                region.put(new Integer(1), new String("first"));
                region.put(new Integer(2), new String("second"));
                region.put(new Integer(3), new String("third"));
                region.clear();
                assertEquals(0, region.size());
            }
        }
        );
        
        vm1.invoke(new CacheSerializableRunnable("temp1vm1"){
            public void run2()throws CacheException{
                assertEquals(0, region.size());
            }
        }
        );
        
        //verifying Single VM and single transaction clear functionalities
        vm1.invoke(new CacheSerializableRunnable("temp2"){
            public void run2() throws CacheException{
                try{
                    region.put(new Integer(1), new String("first"));
                    region.put(new Integer(2), new String("second"));
                    region.put(new Integer(3), new String("third"));
                    cacheTxnMgr = cache.getCacheTransactionManager();
                    cacheTxnMgr.begin();
                    region.put(new Integer(4), new String("forth"));
                    try {
                      region.clear();
                      fail("expected exception not thrown");
                    } catch (UnsupportedOperationInTransactionException e) {
                      // expected
                    }
                    region.put(new Integer(5), new String("fifth"));
                    cacheTxnMgr.commit();
                    assertEquals(5, region.size());
                    assertEquals("fifth", region.get(new Integer(5)).toString());
                }
                catch(CacheException ce){
                    ce.printStackTrace();
                }
                finally{
                    if(cacheTxnMgr.exists()){
                        try {cacheTxnMgr.commit();} catch (Exception cce){cce.printStackTrace();}
                    }
                }
                
            }
        }
        );
        
        //verifying that region.clear does not clear the entries from sub region
        vm0.invoke(new CacheSerializableRunnable("temp3"){
            public void run2()throws CacheException{
                region.put(new Integer(1), new String("first"));
                region.put(new Integer(2), new String("second"));
                AttributesFactory factory  = new AttributesFactory();
                factory.setScope(Scope.DISTRIBUTED_ACK);
                RegionAttributes attr = factory.create();
                Region subRegion = region.createSubregion("subr", attr);
                subRegion.put(new Integer(3), new String("third"));
                subRegion.put(new Integer(4), new String("forth"));
                region.clear();
                assertEquals(0, region.size());
                assertEquals(2, subRegion.size());
            }
        }
        );
        
    }//end of test case
    
    public void testClearMultiVM() throws Throwable{
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        //put 3 key/values in vm0 and get from vm1
        Object[] objArr = new Object[1];
        //Integer in = new Integer(i);
        //objArr[0] = (Object) in;
        for (int i=1; i<4; i++){
            objArr[0] = ""+i;
            vm0.invoke(ClearMultiVmDUnitTest.class, "putMethod", objArr);
            vm1.invoke(ClearMultiVmDUnitTest.class, "getMethod", objArr);
        }
        
        AsyncInvocation as1 = vm0.invokeAsync(ClearMultiVmDUnitTest.class, "firstVM");
        AsyncInvocation as2 = vm1.invokeAsync(ClearMultiVmDUnitTest.class, "secondVM");
        DistributedTestCase.join(as1, 30 * 1000, getLogWriter());
        DistributedTestCase.join(as2, 30 * 1000, getLogWriter());
        
        if(as1.exceptionOccurred()){
          fail("as1 failed", as1.getException());
        }
        
        if(as2.exceptionOccurred()){
          fail("as2 failed", as2.getException());
        }
        
        int j = vm0.invokeInt(ClearMultiVmDUnitTest.class, "sizeMethod");
        assertEquals(0, j);
        
        j = vm1.invokeInt(ClearMultiVmDUnitTest.class, "sizeMethod");
        assertEquals(1, j);
        
        
        
        int i=6;
        objArr[0] = ""+i;
        vm1.invoke(ClearMultiVmDUnitTest.class, "getMethod", objArr);
        
        Object ob[] = new Object[1];
        ob[0] = "secondVM";
        boolean val = vm1.invokeBoolean(ClearMultiVmDUnitTest.class, "containsValueMethod", ob);
        assertEquals(true, val);
        
    }//end of testClearMultiVM
    
    public void testClearExceptions(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        vm1.invoke(ClearMultiVmDUnitTest.class, "localDestroyRegionMethod");
        vm0.invoke(new CacheSerializableRunnable ("exception in vm0"){
            public void run2() throws CacheException {
               try{
                    region.clear();                    
                } catch(RegionDestroyedException rdex){
                    fail("Should NOT have thrown RegionDestroyedException");
                } 
            }
        }        
        );
        
        vm1.invoke(new CacheSerializableRunnable ("exception in vm1"){
            public void run2() throws CacheException {
               try{
                    region.clear();
                    fail("Should have thrown RegionDestroyedException");
                } catch(RegionDestroyedException rdex){
                    //pass
                } 
            }
        }        
        );
        
    }//end of testClearExceptions

    public void testGiiandClear() throws Throwable{
      if (false) {
        getSystem().getLogWriter().severe("testGiiandClear skipped because of bug 34963");
      } else {
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        SerializableRunnable create = new
        CacheSerializableRunnable("create mirrored region") {
            public void run2() throws CacheException {
                AttributesFactory factory1  = new AttributesFactory();
                factory1.setScope(Scope.DISTRIBUTED_ACK);
                factory1.setDataPolicy(DataPolicy.REPLICATE);
                RegionAttributes attr1 = factory1.create();
                mirroredRegion = cache.createRegion("mirrored", attr1);
                // reset slow
                com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = 0;
            }
        };
        
        
        vm0.invoke(create);
        
        vm0.invoke(new CacheSerializableRunnable("put initial data"){
            public void run2() throws CacheException {
                for(int i=0; i<1000; i++){
                    mirroredRegion.put(new Integer(i), (new Integer(i)).toString());
                }
            }
        }
        );
        
        // slow down image processing to make it more likely to get async updates
        vm1.invoke(new SerializableRunnable("set slow image processing") {
            public void run() {
                // if this is a no_ack test, then we need to slow down more because of the
                // pauses in the nonblocking operations
                int pause = 50;
                com.gemstone.gemfire.internal.cache.InitialImageOperation.slowImageProcessing = pause;
            }
        });
        
        // now do the get initial image in vm1
        AsyncInvocation async1 = vm1.invokeAsync(create);

        // try to time a distributed clear to happen in the middle of gii
        vm0.invoke(new SerializableRunnable("call clear when gii") {
            public void run() {
                try{Thread.sleep(3*1000);}catch(InterruptedException ex){fail("interrupted");}
                mirroredRegion.clear();                
                assertEquals(0, mirroredRegion.size());
            }
        });
        
        DistributedTestCase.join(async1, 30 * 1000, getLogWriter());
        if(async1.exceptionOccurred()){
          fail("async1 failed", async1.getException());
        }
        
        SerializableRunnable validate = new
        CacheSerializableRunnable("validate for region size") {
            public void run2() throws CacheException {
                assertEquals(0, mirroredRegion.size());
            }
        };
        
        vm0.invoke(validate);
        vm1.invoke(validate);
      }
        
    }//end of testGiiandClear
    
    
    //remote vm methods
    
    public static void firstVM(){
          //put one entry
          int i=5;
          region.put(i, "firstVM");
            
          //test localClear
          region.localClear();
          assertFalse(region.containsKey(i));
          
          paperWork.put("localClear","true");
          
          // wait unit clear on 2nd VM
          boolean val=false;
          while (!val) {
            val = paperWork.containsKey("localClearVerified");
          }
          region.put("key10", "value");
          region.put("key11", "value");
          
          // test distributed clear
          region.clear();
          paperWork.put("clear", "value");
          
          val = false;
          while (!val) {
            val = paperWork.containsKey("clearVerified");
          }
          
    }//end of firstVM
    
    public static void secondVM(){
          // verify that localClear on the other VM does not affect size on this VM
          boolean val=false;
          while (!val) {
            val = paperWork.containsKey("localClear");
          }
          assertEquals(3, region.size());
          
          paperWork.put("localClearVerified", "value");
          
          // wait for clear
          val = false;
          while (!val) {
            val = paperWork.containsKey("clear");
          }
          assertEquals(0, region.size());
          
          paperWork.put("clearVerified", "value");
          region.put(""+6, "secondVM");
    }//end of secondVM
    
    //helper methods
    
    
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
    
    public static void localDestroyRegionMethod(){
        try{
            region.localDestroyRegion();
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }//end of localDestroyRegionMethod
    
    public static void clearExceptions(){
        try{
            region.clear();
            //fail("Should have thrown RegionDestroyedException");
        } catch(RegionDestroyedException rdex){
            //pass
        }
    }//end of clearExceptions
    
}//end of class
