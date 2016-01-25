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
 * RemoveAllMultiVmDUnitTest.java
 *
 * Adapted from PutAllMultiVmDUnitTest
 */
package com.gemstone.gemfire.cache30;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 * @author  darrel
 */
public class RemoveAllMultiVmDUnitTest extends DistributedTestCase {
    
    /** Creates a new instance of RemoveAllMultiVmDUnitTest */
    public RemoveAllMultiVmDUnitTest(String name) {
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
      vm0.invoke(RemoveAllMultiVmDUnitTest.class, "createCache");
      vm1.invoke(RemoveAllMultiVmDUnitTest.class, "createCache");
    }
    
    public void tearDown2(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        vm0.invoke(RemoveAllMultiVmDUnitTest.class, "closeCache");
        vm1.invoke(RemoveAllMultiVmDUnitTest.class, "closeCache");
        cache = null;
        invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
    }
    
    public static void createCache(){
        try{
            ds = (new RemoveAllMultiVmDUnitTest("temp")).getSystem(props);
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
    
    public void testLocalRemoveAll(){
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
    
      vm0.invoke(new CacheSerializableRunnable("testLocalRemoveAll"){
          public void run2() throws CacheException {
              int cntr = 0, cntr1 = 0;
              for(int i=1; i<6; i++) {
                  region.put(Integer.valueOf(i), new String("testLocalRemoveAll"+i));
                  cntr++;
              }
              
              int size1 = region.size();
              assertEquals(5, size1);
              
              region.removeAll(Collections.EMPTY_SET);
              assertEquals(size1, region.size());
              region.removeAll(Collections.singleton(Integer.valueOf(666)));
              assertEquals(size1, region.size());
              assertEquals(true, region.containsKey(Integer.valueOf(1)));
              region.removeAll(Collections.singleton(Integer.valueOf(1)));
              assertEquals(false, region.containsKey(Integer.valueOf(1)));
              assertEquals(size1-1, region.size());
              size1--;
              region.removeAll(Arrays.asList(Integer.valueOf(2), Integer.valueOf(3)));
              assertEquals(size1-2, region.size());
              size1 -= 2;
         }
      } );
    }
    
    public void testLocalTxRemoveAll(){
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
    
      vm0.invoke(new CacheSerializableRunnable("testSimpleRemoveAllTx"){
        public void run2() throws CacheException {
            cacheTxnMgr = cache.getCacheTransactionManager();
            int cntr = 0;
            for(int i=1; i<6; i++) {
                region.put(Integer.valueOf(i), new String("testLocalTxRemoveAll"+i));
                cntr++;
            }
            
            int size1 = region.size();
            assertEquals(5, size1);
            
            cacheTxnMgr.begin();
            region.removeAll(Arrays.asList(Integer.valueOf(1), Integer.valueOf(2)));
            cacheTxnMgr.rollback();
            
            assertEquals(size1, region.size());
            
            cacheTxnMgr.begin();
            region.removeAll(Arrays.asList(Integer.valueOf(666), Integer.valueOf(1), Integer.valueOf(2)));
            cacheTxnMgr.commit();
            
            int size2 = region.size();
            
            assertEquals(size1-2, size2);
            assertEquals(true, region.containsKey(Integer.valueOf(3)));
            assertEquals(false, region.containsKey(Integer.valueOf(2)));
            assertEquals(false, region.containsKey(Integer.valueOf(1)));
        }
    } );
    }

    public void testDistributedRemoveAll(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        vm1.invoke(new CacheSerializableRunnable("create mirrored region"){
          public void run2() throws CacheException {
              createMirroredRegion();
          }
        });
      
        vm0.invoke(new CacheSerializableRunnable("testDistributedRemoveAll1"){
            public void run2() throws CacheException {
                createMirroredRegion();
                int cntr = 0, cntr1 = 0;
                for(int i=1; i<6; i++) {
                  mirroredRegion.put(Integer.valueOf(i), new String("testDistributedRemoveAll"+i));
                    cntr++;
                }
                
                int size1 = mirroredRegion.size();
                assertEquals(5, size1);
                
                mirroredRegion.removeAll(Collections.EMPTY_SET);
                assertEquals(size1, mirroredRegion.size());
                mirroredRegion.removeAll(Collections.singleton(Integer.valueOf(666)));
                assertEquals(size1, mirroredRegion.size());
                assertEquals(true, mirroredRegion.containsKey(Integer.valueOf(1)));
                mirroredRegion.removeAll(Collections.singleton(Integer.valueOf(1)));
                assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(1)));
                assertEquals(size1-1, mirroredRegion.size());
                size1--;
                mirroredRegion.removeAll(Arrays.asList(Integer.valueOf(2), Integer.valueOf(3)));
                assertEquals(size1-2, mirroredRegion.size());
                size1 -= 2;
           }
        } );
        
        vm1.invoke(new CacheSerializableRunnable("testDistributedRemoveAllVerifyRemote"){
          public void run2() throws CacheException {
            assertEquals(true, mirroredRegion.containsKey(Integer.valueOf(5)));
            assertEquals(true, mirroredRegion.containsKey(Integer.valueOf(4)));
            assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(3)));
            assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(2)));
            assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(1)));
            assertEquals(2, mirroredRegion.size());
          }
        } );
    }
    public void testDistributedTxRemoveAll(){
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      
      vm1.invoke(new CacheSerializableRunnable("create mirrored region"){
        public void run2() throws CacheException {
            createMirroredRegion();
        }
      });
    
      vm0.invoke(new CacheSerializableRunnable("testDistributedTxRemoveAll1"){
          public void run2() throws CacheException {
              createMirroredRegion();
              int cntr = 0, cntr1 = 0;
              for(int i=1; i<6; i++) {
                mirroredRegion.put(Integer.valueOf(i), new String("testDistributedTxRemoveAll"+i));
                  cntr++;
              }
              
              int size1 = mirroredRegion.size();
              assertEquals(5, size1);
              cacheTxnMgr = cache.getCacheTransactionManager();
              
              cacheTxnMgr.begin();
             mirroredRegion.removeAll(Collections.EMPTY_SET);
             cacheTxnMgr.commit();
              assertEquals(size1, mirroredRegion.size());
              cacheTxnMgr.begin();
              mirroredRegion.removeAll(Collections.singleton(Integer.valueOf(666)));
              cacheTxnMgr.commit();
              assertEquals(size1, mirroredRegion.size());
              assertEquals(true, mirroredRegion.containsKey(Integer.valueOf(1)));
              cacheTxnMgr.begin();
              mirroredRegion.removeAll(Collections.singleton(Integer.valueOf(1)));
              cacheTxnMgr.commit();
              assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(1)));
              assertEquals(size1-1, mirroredRegion.size());
              size1--;
              cacheTxnMgr.begin();
              mirroredRegion.removeAll(Arrays.asList(Integer.valueOf(2), Integer.valueOf(3)));
              cacheTxnMgr.commit();
              assertEquals(size1-2, mirroredRegion.size());
              size1 -= 2;
         }
      } );
      
      vm1.invoke(new CacheSerializableRunnable("testDistributedTxRemoveAllVerifyRemote"){
        public void run2() throws CacheException {
          assertEquals(true, mirroredRegion.containsKey(Integer.valueOf(5)));
          assertEquals(true, mirroredRegion.containsKey(Integer.valueOf(4)));
          assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(3)));
          assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(2)));
          assertEquals(false, mirroredRegion.containsKey(Integer.valueOf(1)));
          assertEquals(2, mirroredRegion.size());
        }
      } );
  }
     
}//end of RemoveAllMultiVmDUnitTest
