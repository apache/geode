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
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Disk Reg DUNIT Test:
 * A byte array value when put in a remote VM , gets pushed to local VM as a 
 * VMCachedDeserializable object & that should get persisted in the DiskRegion correctly.
 * The value when obtained from the disk ,locally , 
 * should be correctly obtained as ByteArrray.
 * 
 * @author Vikram Jadhav
 */

public class DiskRegByteArrayDUnitTest extends CacheTestCase {
  static Cache cache;
  static Properties props = new Properties();
  static Properties propsWork = new Properties();
  static DistributedSystem ds = null;
  static Region region;
  static CacheTransactionManager cacheTxnMgr;
  protected static File[] dirs = null;
  final static byte[] value = new byte[1024];
  
   
    public DiskRegByteArrayDUnitTest(String name) {
        super(name);
        File file1 = new File( name + "1");
        file1.mkdir();
        file1.deleteOnExit();
        File file2 = new File( name + "2");
        file2.mkdir();
        file2.deleteOnExit();
        File file3 = new File( name + "3");
        file3.mkdir();
        file3.deleteOnExit();
        File file4 = new File( name + "4");
        file4.mkdir();
        file4.deleteOnExit();
        dirs = new File[4];
        dirs[0] = file1;
        dirs[1] = file2;
        dirs[2] = file3;
        dirs[3] = file4;
        
    }
    
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(DiskRegByteArrayDUnitTest.class, "createCacheForVM0");
      vm1.invoke(DiskRegByteArrayDUnitTest.class, "createCacheForVM1");
     }
    
    @Override
    protected final void postTearDownCacheTestCase() throws Exception {
      cache = null;
      Invoke.invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
    }

    /* public void tearDown(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        vm0.invoke(DiskRegByteArrayDUnitTest.class, "deleteFiles");
        vm1.invoke(DiskRegByteArrayDUnitTest.class, "deleteFiles");
        vm0.invoke(DiskRegByteArrayDUnitTest.class, "closeCache");
        vm1.invoke(DiskRegByteArrayDUnitTest.class, "closeCache");
    }*/
    
    public static void createCacheForVM0(){
        try{
            ds = (new DiskRegByteArrayDUnitTest("vm0_diskReg")).getSystem(props);
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
            factory.setDiskSynchronous(false);
            factory.setDiskStoreName(cache.createDiskStoreFactory()
                                     .setDiskDirs(dirs)
                                     .create("DiskRegByteArrayDUnitTest")
                                     .getName());
            RegionAttributes attr = factory.create();
            region = cache.createVMRegion("region", attr);
        } catch (Exception ex){
            ex.printStackTrace();
            fail(ex.toString());
        }
    }
    
    public static void createCacheForVM1(){
        try{
            
            ds = (new DiskRegByteArrayDUnitTest("vm1_diskReg")).getSystem(props);
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
            factory.setScope(Scope.DISTRIBUTED_ACK);
            factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
            factory.setDiskSynchronous(false);
            factory.setDiskStoreName(cache.createDiskStoreFactory()
                                     .setDiskDirs(dirs)
                                     .create("DiskRegByteArrayDUnitTest")
                                     .getName());
            RegionAttributes attr = factory.create();
            region = cache.createVMRegion("region", attr);
        } catch (Exception ex){
            ex.printStackTrace();
            fail(ex.toString());
        }
    }
    /*
    public static void closeCache(){
        try{
            cache.close();
            ds.disconnect();
        } catch (Exception ex){
            ex.printStackTrace();
            fail(ex.toString());
        }
    }*/
    
    //test methods
 
    public void testPutGetByteArray(){
        
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
       Object[] objArr = new Object[1];
       objArr[0] = "key";

       //Put in vm0 
       vm0.invoke(DiskRegByteArrayDUnitTest.class, "putMethod", objArr);
       //forceflush data to disk
        vm1.invoke(DiskRegByteArrayDUnitTest.class, "flushMethod");
      /* get the val from disk
       * verify that the value retrieved from disk represents a byte[]
       * 
       */
        vm1.invoke(DiskRegByteArrayDUnitTest.class, "verifyByteArray", objArr);
        
        
    }//end of test case1
    
    
    public static Object putMethod(Object ob){
        Object obj=null;
        try{
            if(ob != null){
              Arrays.fill(value , (byte)77);
              obj = region.put(ob, value);
            }
        }catch(Exception ex){
            ex.printStackTrace();
            fail("Failed while region.put");
        }
        return obj;
    }//end of putMethod
    
    public static Object getMethod(Object ob){
      Object obj=null;
      try{
          obj = region.get(ob);
      } catch(Exception ex){
          fail("Failed while region.get");
      }
      return obj;
  } // end of getMethod
      
    
    public static Object getValueFromDiskMethod(Object ob){
        Object val =null;
        //get from disk   
        try {
            DiskId diskId = ((DiskEntry)(((LocalRegion)region).basicGetEntry(ob)))
              .getDiskId();
          val = ((LocalRegion)region).getDiskRegion().get(diskId);
        }
        catch (Exception ex) {
          ex.printStackTrace();
          fail("Failed to get the value on disk");

        }
        return val;
        
    }//end of getValueFromDiskMethod
    
    public static boolean verifyByteArray(Object ob){
      boolean result = false;
     Object val =null;
      Arrays.fill(value , (byte)77);
      //get from disk   
      try {
      DiskId diskId = ((DiskEntry)(((LocalRegion)region).basicGetEntry(ob))).getDiskId();
        val = ((LocalRegion)region).getDiskRegion().get(diskId);
      }
      catch (Exception ex) {
        ex.printStackTrace();
        fail("Failed to get the value on disk");

      }
      assertTrue("The value retrieved from disk is not a byte[] " +
        		"or the length of byte[] is not equla to the length set while put",
        		(((byte[])val).length)== (value.length) );

         
      byte [] x = null;
      x = (byte [])val;
     
     for (int i=0; i < x.length; i++  ){
       result = (x[i] == value[i]); 
     //  System.out.println("*********"+result);
     }
     
     
     return result;
      
    }//end of verifyByteArray
     /**
      * Force flush the data to disk
      *
      */  
    public static void flushMethod(){
        try{
          ((LocalRegion)region).getDiskRegion().forceFlush();
        } catch(Exception ex){
            ex.printStackTrace();
        }
    }  
    
}// end of class

