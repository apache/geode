/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * RemoveAllDAckDunitTest.java
 *
 * Created on September 15, 2005, 5:51 PM
 */

package com.gemstone.gemfire.internal.cache;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedSystem;

import dunit.*;

import java.util.ArrayList;
import java.util.Properties;
/**
 * Adapted from RemoveAllDAckDUnitTest
 * @author darrel
 */
public class RemoveAllDAckDUnitTest extends DistributedTestCase {
    
    /** Creates a new instance of RemoveAllDAckDunitTest */
    public RemoveAllDAckDUnitTest(String name) {
        super(name);
    }
    static Cache cache;
    static Properties props = new Properties();
    static Properties propsWork = new Properties();
    static DistributedSystem ds = null;
    static Region region;
    static boolean beforeDestroy=false;
    static int beforeDestroyRemoveAllcounter = 0;
    
    static boolean flag = false;
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(RemoveAllDAckDUnitTest.class, "createCacheForVM0");
      vm1.invoke(RemoveAllDAckDUnitTest.class, "createCacheForVM1");
      getLogWriter().fine("Cache created successfully");
    }
    
    public void tearDown2(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        vm0.invoke(RemoveAllDAckDUnitTest.class, "closeCache");
        vm1.invoke(RemoveAllDAckDUnitTest.class, "closeCache");
    }
    
    public static void createCacheForVM0() throws Exception {
            ds = (new RemoveAllDAckDUnitTest("temp")).getSystem(props);
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            RegionAttributes attr = factory.create();
            region = cache.createRegion("map", attr);
    }
    
    public static void createCacheForVM1() throws Exception {
            CacheWriter aWriter = new BeforeDestroyCallback();
            ds = (new RemoveAllDAckDUnitTest("temp")).getSystem(props);
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
 
    public void testRemoveAllRemoteVM() {
        // Test PASS. 
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        Object[] objArr = new Object[1];
        for (int i=0; i<3; i++){
            objArr[0] = ""+i;
            vm0.invoke(RemoveAllDAckDUnitTest.class, "putMethod", objArr);
        }
        vm0.invoke(RemoveAllDAckDUnitTest.class, "removeAllMethod");
        flag = vm1.invokeBoolean(RemoveAllDAckDUnitTest.class,"getFlagVM1");
        assertEquals(true, flag);
        
        vm1.invoke(new CacheSerializableRunnable("temp1"){
            public void run2() throws CacheException{
              assertEquals(2, beforeDestroyRemoveAllcounter);
            }
        });
        
    }//end of test case1
    
    
    public static Object putMethod(Object ob) {
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
    
    public static void removeAllMethod() {
      assertEquals(3, region.size());
      ArrayList l = new ArrayList();
      for (int i=0; i<2; i++){
        l.add(""+i);
      }
      region.removeAll(l, "removeAllCallback");
      assertEquals(1, region.size());
    }
    
    static class BeforeDestroyCallback extends CacheWriterAdapter {
        public void beforeDestroy(EntryEvent event){
            beforeDestroyRemoveAllcounter++;
            assertEquals(true, event.getOperation().isRemoveAll());
            assertEquals("removeAllCallback", event.getCallbackArgument());
            getLogWriter().fine("*******BeforeDestroy*****");
            beforeDestroy = true;
        }
    }
    public static boolean getFlagVM1(){
        return beforeDestroy;
    }
    
}// end of class
