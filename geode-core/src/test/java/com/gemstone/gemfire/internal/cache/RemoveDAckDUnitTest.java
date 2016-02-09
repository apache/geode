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
 * RemoveDAckDUnitTest.java
 *
 * Created on September 15, 2005, 12:41 PM
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 * @author vjadhav
 */
public class RemoveDAckDUnitTest extends DistributedTestCase {
    
    /** Creates a new instance of RemoveDAckDUnitTest */
    public RemoveDAckDUnitTest(String name) {
        super(name);
    }
    
    static Cache cache;
    static Properties props = new Properties();
    static Properties propsWork = new Properties();
    static DistributedSystem ds = null;
    static Region region;
    static CacheTransactionManager cacheTxnMgr;
    static volatile  boolean IsBeforeDestroy = false;
    static boolean flag = false;
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(RemoveDAckDUnitTest.class, "createCacheVM0");
      vm1.invoke(RemoveDAckDUnitTest.class, "createCacheVM1");
      LogWriterUtils.getLogWriter().fine("Cache created in successfully");
    }
    
    @Override
    protected final void preTearDown() throws Exception {
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(RemoveDAckDUnitTest.class, "closeCache");
      vm1.invoke(RemoveDAckDUnitTest.class, "closeCache");
    }
    
    public static void createCacheVM0(){
        try{            
            ds = (new RemoveDAckDUnitTest("temp")).getSystem(props);
            cache = CacheFactory.create(ds);            
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);            
            RegionAttributes attr = factory.create();
            
            region = cache.createRegion("map", attr);
        } catch (Exception ex){
            ex.printStackTrace();
        }
    } //end of create cache for VM0
    public static void createCacheVM1(){
        try{            
            ds = (new RemoveDAckDUnitTest("temp")).getSystem(props);
            AttributesFactory factory  = new AttributesFactory();
            cache = CacheFactory.create(ds);
            factory.setScope(Scope.DISTRIBUTED_ACK);            
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
    
    
    public void testRemoveMultiVM(){
        //Commented the Test.As it is failing @ line no 133 : AssertionFailedError
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
//        Object obj1;
        Object[] objArr = new Object[1];
        for (int i=1; i<5; i++){
            objArr[0] = new Integer(i);
            vm0.invoke(RemoveDAckDUnitTest.class, "putMethod", objArr);
        }
        
        vm1.invoke(new CacheSerializableRunnable("get object"){
                public void run2() throws CacheException{
                    for (int i=1; i<5; i++){
                        region.get(new Integer(i));
                    }
                }
            });
        
        
        int i=2;
        objArr[0] = new Integer(i);
        vm0.invoke(RemoveDAckDUnitTest.class,"removeMethod", objArr);
        
        int Regsize = vm1.invokeInt(RemoveDAckDUnitTest.class, "sizeMethod");
        assertEquals(3, Regsize);
        
    }//end of test case
    
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
    public static int sizeMethod(){
        int i=0;
        try{
            i = region.size();
        }catch(Exception ex){
            fail("Failed while region.size");
        }
        return i;
    }
    
    public static Object removeMethod(Object obR){
        Object objR=null;
        try{
            if(obR != null){
                objR = region.remove(obR);
            }
        }catch(Exception ex){
            ex.printStackTrace();
            fail("Failed while region.remove");
        }
        return objR;
        
    }
    

}// end of  class














