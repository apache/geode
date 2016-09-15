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
 * Bug33359DUnitTest.java
 *
 * Created on September 6, 2005, 2:57 PM
 */
package org.apache.geode.internal.cache;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class Bug33359DUnitTest extends JUnit4DistributedTestCase { // TODO: reformat
    
    static Cache cache;
    static Properties props = new Properties();
    static Properties propsWork = new Properties();
    static DistributedSystem ds = null;
    static Region region;
    static Region paperWork;
    static CacheTransactionManager cacheTxnMgr;
    static boolean IsAfterClear=false;
    static boolean flag = false;
    
    @Override
    public final void postSetUp() throws Exception {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(() -> Bug33359DUnitTest.createCacheVM0());
      vm1.invoke(() -> Bug33359DUnitTest.createCacheVM1());
      LogWriterUtils.getLogWriter().fine("Cache created in successfully");
    }
    
    @Override
    public final void preTearDown(){
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(() -> Bug33359DUnitTest.closeCache());
      vm1.invoke(() -> Bug33359DUnitTest.closeCache());
    }
    
    public static void createCacheVM0(){
        try{
            ds = (new Bug33359DUnitTest()).getSystem(props);
            cache = CacheFactory.create(ds);
            
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.REPLICATE);
            factory.setEarlyAck(true);
            DistributedSystem.setThreadsSocketPolicy(false);
            RegionAttributes attr = factory.create();
            
            region = cache.createRegion("map", attr);
            paperWork = cache.createRegion("paperWork", attr);
        } catch (Exception ex){
            ex.printStackTrace();
        }
    } //end of create cache for VM0
    public static void createCacheVM1(){
        try{
            ds = (new Bug33359DUnitTest()).getSystem(props);
            DistributedSystem.setThreadsSocketPolicy(false);
            
            cache = CacheFactory.create(ds);
            
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.REPLICATE);
            
            RegionAttributes attr = factory.create();
            
            region = cache.createRegion("map", attr);
            paperWork = cache.createRegion("paperWork", attr);
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
    
    
  @Test
  public void testClearMultiVM(){
        
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
//        VM vm1 = host.getVM(1);
        
       vm0.invoke(new CacheSerializableRunnable("put initial data"){
            public void run2() throws CacheException {
                for(int i=0; i<10; i++){
                    region.put(new Integer(i), Integer.toString(i));
                }                
                LogWriterUtils.getLogWriter().fine("Did all puts successfully");
            }
        }
        );        
        
        vm0.invoke(new CacheSerializableRunnable("perform clear on region"){
            public void run2() throws CacheException {
                region.clear();
                LogWriterUtils.getLogWriter().fine("region is cleared");
            }
        }
        );        
        
        
    }//end of test case    
    
}// end of test class
