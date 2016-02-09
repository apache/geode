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
 * PutAllMultiVmDUnitTest.java
 *
 * Created on September 1, 2005, 12:19 PM
 */
package com.gemstone.gemfire.cache30;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

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
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 * @author  prafulla
 */
public class PutAllMultiVmDUnitTest extends DistributedTestCase{
    
    /** Creates a new instance of PutAllMultiVmDUnitTest */
    public PutAllMultiVmDUnitTest(String name) {
        super(name);
    }
    
    static Cache cache;
    static Properties props = new Properties();
    static Properties propsWork = new Properties();
    static DistributedSystem ds = null;
    static Region region;
    static Region mirroredRegion;
    static CacheTransactionManager cacheTxnMgr;
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(PutAllMultiVmDUnitTest.class, "createCache");
      vm1.invoke(PutAllMultiVmDUnitTest.class, "createCache");
    }
    
    @Override
    protected final void preTearDown() throws Exception {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(PutAllMultiVmDUnitTest.class, "closeCache");
      vm1.invoke(PutAllMultiVmDUnitTest.class, "closeCache");
      cache = null;
      Invoke.invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
    }
    
    public static void createCache(){
        try{
            ds = (new PutAllMultiVmDUnitTest("temp")).getSystem(props);
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            RegionAttributes attr = factory.create();
            region = cache.createRegion("map", attr);
            
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }//end of createCache
    
    public static void createMirroredRegion(){
        try{
            AttributesFactory factory  = new AttributesFactory();
            factory.setDataPolicy(DataPolicy.REPLICATE);
            factory.setScope(Scope.DISTRIBUTED_ACK);
            RegionAttributes attr = factory.create();
            mirroredRegion = cache.createRegion("mirrored", attr);
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }//end of createCache
    
    public static void closeCache(){
        try{
            //System.out.println("closing cache cache cache cache cache 33333333");
            cache.close();
            ds.disconnect();
            //System.out.println("closed cache cache cache cache cache 44444444");
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }//end of closeCache
    
    
    //tests methods
    
    public void testSimplePutAll(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        SerializableRunnable clear = new CacheSerializableRunnable("clear"){
            public void run2() throws CacheException {
                try{
                    region.clear();
                }catch(Exception ex){
                    ex.printStackTrace();
                }
            }
        };//end of clear
        
        vm0.invoke(new CacheSerializableRunnable("testSimplePutAll1"){
            public void run2() throws CacheException {
                int cntr = 0, cntr1 = 0;
                for(int i=1; i<6; i++) {
                    region.put(new Integer(i), new String("testSimplePutAll"+i));
                    cntr++;
                }
                
                int size1 = region.size();
                Map m = new HashMap();
                for(int i=6; i<27; i++) {
                    m.put(new Integer(i), new String("map"+i));
                    cntr++;
                    cntr1++;
                }
                
                region.putAll(m);
                int size2 = region.size();
                
                assertEquals(cntr, region.size());
                assertEquals(cntr1, (size2 - size1));
                assertEquals(true, region.containsKey(new Integer(10)));
                assertEquals(true, region.containsValue(new String("map12")));
            }
        } );
        
        vm0.invoke(clear);
        
        vm1.invoke(new CacheSerializableRunnable("create mirrored region"){
            public void run2() throws CacheException {
                createMirroredRegion();
            }
        }
        );
        
        vm0.invoke(new CacheSerializableRunnable("testSimplePutAll2"){
            public void run2() throws CacheException {
                //assertEquals(0, region.size());
                createMirroredRegion();
                cacheTxnMgr = cache.getCacheTransactionManager();
                int cntr = 0;
                for(int i=1; i<6; i++) {
                    mirroredRegion.put(new Integer(i), new String("testSimplePutAll"+i));
                    cntr++;
                }
                
                int size1 = mirroredRegion.size();
                Map m = new HashMap();
                for(int i=6; i<27; i++) {
                    m.put(new Integer(i), new String("map"+i));
                    cntr++;
                }
                
                //Disabled until putAll works in tx
                //cacheTxnMgr.begin();
                //mirroredRegion.putAll(m);
                //cacheTxnMgr.rollback();
                
                assertEquals(size1, mirroredRegion.size());
                assertEquals(false, mirroredRegion.containsKey(new Integer(10)));
                assertEquals(false, mirroredRegion.containsValue(new String("map12")));
                
                //cacheTxnMgr.begin();
                mirroredRegion.putAll(m);
                //cacheTxnMgr.commit();
                
//                int size2 = mirroredRegion.size();
                
                assertEquals(cntr, mirroredRegion.size());
                assertEquals(true, mirroredRegion.containsKey(new Integer(10)));
                assertEquals(true, mirroredRegion.containsValue(new String("map12")));
                
                //sharing the size of region of vm0 in vm1
                mirroredRegion.put("size", new Integer(mirroredRegion.size()));
            }
        } );
        
        vm1.invoke(new CacheSerializableRunnable("testSimplePutAll3"){
            public void run2() throws CacheException {
                Integer i = (Integer) mirroredRegion.get("size");
                int cntr = i.intValue();
                assertEquals(cntr, (mirroredRegion.size()-1));
                assertEquals(true, mirroredRegion.containsKey(new Integer(10)));
                assertEquals(true, mirroredRegion.containsValue(new String("map12")));
            }
        } );
        
    }//end of testSimplePutAll
    
    public void testPutAllExceptions(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        vm0.invoke(new CacheSerializableRunnable("testPutAllExceptions1"){
            public void run2() throws CacheException {
                int cntr = 0;
//                int cntr1 = 0;
                for(int i=1; i<6; i++) {
                    region.put(new Integer(i), new String("testSimplePutAll"+i));
                    cntr++;
                }
                
                Map m = new TreeMap();//to verify the assertions
                for(int i=6; i<27; i++) {
                    if(i == 16){
                        m.put(new Integer(i), null);
                    }
                    else {
                        m.put(new Integer(i), new String("map"+i));
                    }
                }
                
                try{
                    region.putAll(m);
                    fail("Expect NullPointerException");
                } catch (NullPointerException ex){
                    //do nothing
                }
                
                assertEquals(5, region.size());
                assertEquals(false, region.containsKey(new Integer(10)));
                assertEquals(false, region.containsValue(new String("map12")));
                assertEquals(false, region.containsKey(new Integer(20)));
                assertEquals(false, region.containsValue(new String("map21")));
            }
        } );
        
        
        vm1.invoke(new CacheSerializableRunnable("create mirrored region"){
            public void run2() throws CacheException {
                createMirroredRegion();
            }
        }
        );
        
        
        vm0.invoke(new CacheSerializableRunnable("testPutAllExceptions2"){
            public void run2() throws CacheException {
                //assertEquals(0, region.size());
                createMirroredRegion();
                
                for(int i=1; i<6; i++) {
                    mirroredRegion.put(new Integer(i), new String("testSimplePutAll"+i));
                }
                
                Map m = new TreeMap();//to verify the assertions
                for(int i=6; i<27; i++) {
                    if(i == 16){
                        m.put(new Integer(i), null);
                    }
                    else {
                        m.put(new Integer(i), new String("map"+i));
                    }
                }
                
                try{
                    mirroredRegion.putAll(m);
                    fail("Expect NullPointerException");
                } catch (NullPointerException ex){
                    //do nothing
                }
               
                assertEquals(5, mirroredRegion.size());
                assertEquals(false, mirroredRegion.containsKey(new Integer(10)));
                assertEquals(false, mirroredRegion.containsValue(new String("map12")));
                assertEquals(false, region.containsKey(new Integer(20)));
                assertEquals(false, region.containsValue(new String("map21")));

                //sharing the size of region of vm0 in vm1
                mirroredRegion.put("size", new Integer(mirroredRegion.size()));
            }
        } );
        
        vm1.invoke(new CacheSerializableRunnable("testPutAllExceptions3"){
            public void run2() throws CacheException {
                Integer i = (Integer) mirroredRegion.get("size");
                int cntr = i.intValue();
                assertEquals(cntr, (mirroredRegion.size()-1));
                assertEquals(false, mirroredRegion.containsKey(new Integer(10)));
                assertEquals(false, mirroredRegion.containsValue(new String("map12")));
                assertEquals(false, mirroredRegion.containsKey(new Integer(20)));
                assertEquals(false, mirroredRegion.containsValue(new String("map21")));
            }
        } );
        
        
    }//end of testPutAllExceptions
    
    public void testPutAllExceptionHandling(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
//        VM vm1 = host.getVM(1);
        
        vm0.invoke(new CacheSerializableRunnable("testPutAllExceptionHandling1"){
            public void run2() throws CacheException {
                Map m = new HashMap();
                m = null;
                try{
                    region.putAll(m);
                    fail("Should have thrown NullPointerException");
                }catch (NullPointerException ex){
                    //pass
                }
                
                region.localDestroyRegion();
                try{
                    Map m1 = new HashMap();
                    for(int i=1; i<21; i++) {
                        m1.put(new Integer(i), Integer.toString(i));
                    }
                    
                    region.putAll(m1);
                    fail("Should have thrown RegionDestroyedException");
                }catch (RegionDestroyedException ex){
                    //pass
                }
                
            }
        });
        
    }//testPutAllExceptionHandling
    
    
}//end of PutAllMultiVmDUnitTest
