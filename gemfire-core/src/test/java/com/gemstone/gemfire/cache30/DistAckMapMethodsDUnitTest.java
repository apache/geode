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
 * DistAckMapMethodsDUnitTest.java
 *
 * Created on August 4, 2005, 12:36 PM
 */
package com.gemstone.gemfire.cache30;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

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
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 *
 * @author  prafulla
 */
public class DistAckMapMethodsDUnitTest extends DistributedTestCase{
    static Cache cache;
    static Properties props = new Properties();
    static DistributedSystem ds = null;
    static Region region;
    static Region mirroredRegion;
    static Region remRegion;
    static boolean afterDestroy=false;
    
    
    //helper class referece objects
    static Object afterDestroyObj;
    
    /** Creates a new instance of DistAckMapMethodsDUnitTest */
    public DistAckMapMethodsDUnitTest(String name) {
        super(name);
    }
    
    @Override
    public void setUp() throws Exception {
      super.setUp();
      Host host = Host.getHost(0);
      VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);
      vm0.invoke(DistAckMapMethodsDUnitTest.class, "createCache");
      vm1.invoke(DistAckMapMethodsDUnitTest.class, "createCache");
    }
    
    public void tearDown2(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "closeCache");
        vm1.invoke(DistAckMapMethodsDUnitTest.class, "closeCache");
        cache = null;
        invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
    }
    
    public static void createCache(){
        try{
            //props.setProperty("mcast-port", "1234");
            //ds = DistributedSystem.connect(props);
            ds = (new DistAckMapMethodsDUnitTest("temp")).getSystem(props);
            cache = CacheFactory.create(ds);
            AttributesFactory factory  = new AttributesFactory();
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
    
    public static void createMirroredRegion(){
        try{
            AttributesFactory factory1  = new AttributesFactory();
            factory1.setScope(Scope.DISTRIBUTED_ACK);
            factory1.setDataPolicy(DataPolicy.REPLICATE);
            RegionAttributes attr1 = factory1.create();
            mirroredRegion = cache.createRegion("mirrored", attr1);
            
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
    
    public static void createRegionToTestRemove(){
        try{
            AttributesFactory factory2  = new AttributesFactory();
            factory2.setScope(Scope.DISTRIBUTED_ACK);
            CacheWriter cacheWriter = new RemoveCacheWriter();
            CacheListener cacheListener = new RemoveCacheListener();
            factory2.setCacheWriter(cacheWriter);
            factory2.setCacheListener(cacheListener);
            RegionAttributes attr2 = factory2.create();
            remRegion = cache.createRegion("remove", attr2);
            
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
    
    //testMethods
    
    public void testPutMethod(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        Object obj1;
        //put from one and get from other
        int i=1;
        Object[] objArr = new Object[1];
        objArr[0] = ""+i;
        //Integer in = new Integer(i);
        //objArr[0] = (Object) in;
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
        obj1 = vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);
        if(obj1 == null ){
            fail("region.put(key, value) from one vm does not match with region.get(key) from other vm");
        }
        
        //put from both vms for same key
        i = 2;
        objArr[0] = ""+i;
        //in = new Integer(i);
        //objArr[0] = (Object) in;
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
        obj1 = vm1.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
        if(obj1 != null){//here if some dummy object is returned on first time put then that should be checked
            fail("failed while region.put from both vms for same key");
        }
    }
    
    public void testRemoveMethod(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        Object obj1, obj2;
        boolean ret;
        //put from one and get from other
        int i=1;
        Object objArr[] = new Object[1];
        objArr[0] = ""+i;
        //Integer in = new Integer(i);
        //objArr[0] = (Object) in;
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "removeMethod", objArr);
        //validate if vm0 has that key value entry
        ret = vm0.invokeBoolean(DistAckMapMethodsDUnitTest.class, "containsKeyMethod", objArr);
        if( ret ){//if returned true means that the key is still there
            fail("region.remove failed with distributed ack scope");
        }
        
        //test if the correct value is returned
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
        obj1 = vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);//to make sure that vm1 region has the entry
        obj2 = vm1.invoke(DistAckMapMethodsDUnitTest.class, "removeMethod", objArr);
        getLogWriter().fine("111111111"+obj1);
        getLogWriter().fine("2222222222"+obj2);
        if (obj1 == null)
          fail("region1.getMethod returned null");
        if(!(obj1.equals(obj2))){
            fail("region.remove failed with distributed ack scope");
        }
    }
    
    public void testRemoveMethodDetails(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "createRegionToTestRemove");
        vm1.invoke(DistAckMapMethodsDUnitTest.class, "createRegionToTestRemove");
        
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "removeMethodDetails");
        vm1.invoke(new CacheSerializableRunnable("testRemoveMethodDetails"){
            public void run2() throws CacheException {
                Object ob1 = remRegion.get(new Integer(1));
                assertEquals("beforeDestroy", ob1.toString());
                //wait till listeber switches afterDestroy to true
                //                    while(!afterDestroy){
                //                        //wait
                //                    }
                assertEquals("afterDestroy", remRegion.get(new Integer(3)).toString());
            }
        }
        );
    }//end of testRemoveMethodDetails
    
    public void testIsEmptyMethod(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
//        boolean ret;
        //put from one and get from other
        int i=1;
        Object objArr[] = new Object[1];
        objArr[0] = ""+i;
        //Integer in = new Integer(i);
        //objArr[0] = (Object) in;
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
        boolean val = vm1.invokeBoolean(DistAckMapMethodsDUnitTest.class, "isEmptyMethod");
        if (!val){//val should be true
            fail("Failed in region.isEmpty");
        }
        
        vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);
        boolean val1 = vm1.invokeBoolean(DistAckMapMethodsDUnitTest.class, "isEmptyMethod");
        if (val1){
            fail("Failed in region.isEmpty");
        }
    }
    
    public void testContainsValueMethod(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
//        boolean ret;
        //put from one and get from other
        int i=1;
        Object objArr[] = new Object[1];
        objArr[0] = ""+i;
        //Integer in = new Integer(i);
        //objArr[0] = (Object) in;
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
        Object ob[] = new Object[1];
        ob[0] = "first";
        boolean val = vm1.invokeBoolean(DistAckMapMethodsDUnitTest.class, "containsValueMethod", ob);
        if (val){//val should be false.
            fail("Failed in region.ContainsValue");
        }
        
        vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);
        boolean val1 = vm1.invokeBoolean(DistAckMapMethodsDUnitTest.class, "containsValueMethod", ob);
        if (!val1){//val1 should be true.
            fail("Failed in region.ContainsValue");
        }
    }
    
    public void testKeySetMethod(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        int i=1;
        Object objArr[] = new Object[1];
        objArr[0] = ""+i;
        //Integer in = new Integer(i);
        //objArr[0] = (Object) in;
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
        int temp = vm1.invokeInt(DistAckMapMethodsDUnitTest.class, "keySetMethod");
        if (temp != 0){
            fail("failed in keySetMethodtest method");
        }
        
        vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);//to make sure that vm1 region has the entry
        temp = vm1.invokeInt(DistAckMapMethodsDUnitTest.class, "keySetMethod");
        if (temp == 0){
            fail("failed in keySetMethodtest method");
        }
        //in the above scenarion we can test this for mirrorred region scenarion as well
        temp=0;
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "createMirroredRegion");
        vm1.invoke(DistAckMapMethodsDUnitTest.class, "createMirroredRegion");
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
        temp = vm1.invokeInt(DistAckMapMethodsDUnitTest.class, "keySetMethod");
        if (temp == 0){
            fail("failed in keySetMethodtest method");
        }
    }
    
    
    public void testEntrySetMethod(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        int i=1;
        Object objArr[] = new Object[1];
        objArr[0] = ""+i;
        //Integer in = new Integer(i);
        //objArr[0] = (Object) in;
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
        int temp = vm1.invokeInt(DistAckMapMethodsDUnitTest.class, "entrySetMethod");
        if (temp != 0){
            fail("failed in entrySetMethodtest method");
        }
        
        vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);//to make sure that vm1 region has the entry
        temp = vm1.invokeInt(DistAckMapMethodsDUnitTest.class, "entrySetMethod");
        if (temp == 0){
            fail("failed in entrySetMethodtest method");
        }
        //in the above scenarion we can test this for mirrorred region scenarion as well
        temp=0;
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "createMirroredRegion");
        vm1.invoke(DistAckMapMethodsDUnitTest.class, "createMirroredRegion");
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "putOnMirroredRegion", objArr);
        temp = vm1.invokeInt(DistAckMapMethodsDUnitTest.class, "entrySetMethod");
        if (temp == 0){
            fail("failed in entrySetMethodtest method");
        }
    }
    
    public void testSizeMethod(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        VM vm1 = host.getVM(1);
        
        int i=1, j=0;
        Object objArr[] = new Object[1];
        objArr[0] = ""+i;
        //Integer in = new Integer(i);
        //objArr[0] = (Object) in;
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "putMethod", objArr);
        j = vm1.invokeInt(DistAckMapMethodsDUnitTest.class, "sizeMethod");
        if( j != 0){
            fail("failed in region.size method");
        }
        
        vm1.invoke(DistAckMapMethodsDUnitTest.class, "getMethod", objArr);//to make sure that vm1 region has the entry
        j = vm1.invokeInt(DistAckMapMethodsDUnitTest.class, "sizeMethod");
        if( j == 0){
            fail("failed in region.size method");
        }
    }
    
    public void testallMethodsArgs(){
        Host host = Host.getHost(0);
        VM vm0 = host.getVM(0);
        vm0.invoke(DistAckMapMethodsDUnitTest.class, "allMethodsArgs");
    }
    
    
    //following is the implementation of the methods of Map to use in dunit test cases.
    /*
     *
     *
     */
    
    public static Object putMethod(Object ob){
        Object obj=null;
        try{
            if(ob != null){
                String str = "first";
                obj = region.put(ob, str);
            }
        }catch(Exception ex){
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
    
    public static Object removeMethod(Object ob){
        Object obj=null;
        try{
            obj = region.remove(ob);
        }catch(Exception ex){
            ex.printStackTrace();
            fail("Failed while region.remove");
        }
        return obj;
    }
    
    public static boolean containsKeyMethod(Object ob){
        boolean flag = false;
        try{
            flag = region.containsKey(ob);
        }catch(Exception ex){
            fail("Failed while region.containsKey");
        }
        return flag;
    }
    
    public static boolean isEmptyMethod(){
        boolean flag = false;
        try{
            flag = region.isEmpty();
        }catch(Exception ex){
            fail("Failed while region.isEmpty");
        }
        return flag;
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
    
    public static int keySetMethod(){
        Set set = new HashSet();
        int i=0;
        try{
            set = region.keySet();
            i = set.size();
        }catch(Exception ex){
            ex.printStackTrace();
            fail("Failed while region.keySet");
        }
        return i;
    }
    
    public static int entrySetMethod(){
        Set set = new HashSet();
        int i=0;
        try{
            set = region.entrySet();
            i = set.size();
        }catch(Exception ex){
            ex.printStackTrace();
            fail("Failed while region.entrySet");
        }
        return i;
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
    
    //following are methods for put on and get from mirrored regions
    
    public static Object putOnMirroredRegion(Object ob){
        Object obj=null;
        try{
            String str = "mirror";
            obj = mirroredRegion.put(ob, str);
        }catch(Exception ex){
            ex.printStackTrace();
            fail("Failed while mirroredRegion.put");
        }
        return obj;
    }
    
    public static Object getFromMirroredRegion(Object ob){
        Object obj=null;
        try{
            obj = mirroredRegion.get(ob);
            
        } catch(Exception ex){
            fail("Failed while mirroredRegion.get");
        }
        return obj;
    }
    
    public static void removeMethodDetails(){
        Object ob1;
//        Object ob2;
        Integer inOb1 = new Integer(1);
        try{
            region.put(inOb1, "first");
            ob1 = region.remove(inOb1);
            assertEquals("first", ob1.toString());
        }catch(Exception ex){
            ex.printStackTrace();
        }
        
        //to test EntryNotFoundException
        try{
            region.remove(new Integer(2));
            //fail("Should have thrown EntryNotFoundException");
        }//catch (EntryNotFoundException e){
        catch (Exception e){
            //pass
            //e.printStackTrace();
        }
        
        //to test NullPointerException
        try{
            Integer inOb2 = new Integer(2);
            region.put(inOb2, "second");
            inOb2 = null;
            region.remove(inOb2);
            fail("Should have thrown NullPointerException ");
        }catch (NullPointerException  e){
            //pass
        }
        
        //to test the cache writers and listeners
        try {
            //createRegionToTestRemove();
            Integer inOb2 = new Integer(2);
            remRegion.put(inOb2, "second");
            remRegion.remove(inOb2);
            
            //to test cacheWriter
            inOb2 = new Integer(1);
            assertEquals("beforeDestroy", remRegion.get(inOb2).toString());
            
            //wait till listeber switches afterDestroy to true
            while(!afterDestroy){
            }
            //to test cacheListener
            inOb2 = new Integer(3);
            assertEquals("afterDestroy", remRegion.get(inOb2).toString());
            
            //verify that entryEventvalue is correct for listener
            assertNotNull(afterDestroyObj);
            
        }catch (Exception ex){
            ex.printStackTrace();
        }
        
        
    }//end of removeMethodDetail
    
    public static void allMethodsArgs(){
        //testing args for put method
        try{
            region.put(new Integer(1), new String("first"));
            region.put(new Integer(2), new String("second"));
            region.put(new Integer(3), new String("third"));
            
            //test args for get method
            Object ob1 = region.get(new Integer(1));
            assertEquals("first", ob1.toString());
            
            //test args for containsKey method
            boolean val1 = region.containsKey(new Integer(2));
            assertEquals(true, val1);
            
            //test args for containsKey method
            boolean val2 = region.containsValue(new String("second"));
            //assertEquals(true, val2);
            
            //test args for remove method
            try{
                region.remove(new Integer(3));
            }//catch (EntryNotFoundException ex){
            catch (Exception ex){
                ex.printStackTrace();
                fail("failed while region.remove(new Object())");
            }
            
            //verifying the correct exceptions are thrown by the methods
            
            Object key=null, value=null;
            //testing put method
            try{
                region.put(key, value);
                fail("should have thrown NullPointerException");
            }catch(NullPointerException iex){
                //pass
            }
            
            //testing containsValue method
            try{
                region.containsValue(value);
                fail("should have thrown NullPointerException");
            }catch(NullPointerException iex){
                //pass
            }
            
            //RegionDestroyedException
            key = new Integer(5);
            value = new String("fifth");
            
            region.localDestroyRegion();
            //test put method
            try{
                region.put(key, value);
                fail("should have thrown RegionDestroyedException");
            }catch(RegionDestroyedException iex){
                //pass
            }
            
            //test remove method
            try{
                region.remove(key);
                fail("should have thrown RegionDestroyedException");
            }catch(RegionDestroyedException iex){
                //pass
            }
            
            //test containsValue method
            try{
                region.containsValue(value);
                fail("should have thrown RegionDestroyedException");
            }catch(RegionDestroyedException iex){
                //pass
            }
            
            //test size method
            try{
                region.size();
                fail("should have thrown RegionDestroyedException");
            }catch(RegionDestroyedException iex){
                //pass
            }
            
            //test keySet method
            try{
                region.keySet();
                fail("should have thrown RegionDestroyedException");
            }catch(RegionDestroyedException iex){
                //pass
            }
            
            //test entrySet method
            try{
                region.entrySet();
                fail("should have thrown RegionDestroyedException");
            }catch(RegionDestroyedException iex){
                //pass
            }
            
            
        } catch(Exception ex){
            ex.printStackTrace();
        }
        
    }//end of allMethodsArgs
    
    //helper classes
    
    static class RemoveCacheWriter extends CacheWriterAdapter{
        
        public void beforeDestroy(EntryEvent entryEvent) throws com.gemstone.gemfire.cache.CacheWriterException {
            Integer o1 = new Integer(1);
            remRegion.put(o1, "beforeDestroy");
        }
        
    }//end of RemoveCacheWriter
    
    
    static class RemoveCacheListener extends CacheListenerAdapter{
        
        public void afterDestroy(EntryEvent entryEvent) throws com.gemstone.gemfire.cache.CacheWriterException {
            Integer o1 = new Integer(3);
            remRegion.put(o1, "afterDestroy");
            
            afterDestroyObj = entryEvent.getKey();
            
            //to continue main thread where region.remove has actually occurred
            afterDestroy = true;
        }
        
    }//end of RemoveCacheListener
    
    
}//end of class
