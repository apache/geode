/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Bug33359DUnitTest.java
 *
 * Created on September 6, 2005, 2:57 PM
 */
package com.gemstone.gemfire.internal.cache;


import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import dunit.*;
import java.util.Properties;

/**
 *
 * @author vjadhav
 */
public class Bug33359DUnitTest extends DistributedTestCase {
    
    /** Creates a new instance of Bug33359DUnitTest */
    public Bug33359DUnitTest(String name) {
        super(name);
    }
    
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
    public void setUp() throws Exception {
      super.setUp();
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(Bug33359DUnitTest.class, "createCacheVM0");
      vm1.invoke(Bug33359DUnitTest.class, "createCacheVM1");
      getLogWriter().fine("Cache created in successfully");
    }
    
    public void tearDown2(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        vm0.invoke(Bug33359DUnitTest.class, "closeCache");
        vm1.invoke(Bug33359DUnitTest.class, "closeCache");
        
    }
    
    public static void createCacheVM0(){
        try{
            ds = (new Bug33359DUnitTest("temp")).getSystem(props);
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
            ds = (new Bug33359DUnitTest("temp")).getSystem(props);
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
    
    
    public void testClearMultiVM(){
        
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
//        VM vm1 = host.getVM(1);
        
       vm0.invoke(new CacheSerializableRunnable("put initial data"){
            public void run2() throws CacheException {
                for(int i=0; i<10; i++){
                    region.put(new Integer(i), Integer.toString(i));
                }                
                getLogWriter().fine("Did all puts successfully");
            }
        }
        );        
        
        vm0.invoke(new CacheSerializableRunnable("perform clear on region"){
            public void run2() throws CacheException {
                region.clear();
                getLogWriter().fine("region is cleared");
            }
        }
        );        
        
        
    }//end of test case    
    
}// end of test class
