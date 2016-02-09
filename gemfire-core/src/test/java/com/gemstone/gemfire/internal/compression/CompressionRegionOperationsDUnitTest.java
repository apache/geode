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
package com.gemstone.gemfire.internal.compression;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests basic region operations with compression enabled.
 * 
 * @author rholmes
 */
public class CompressionRegionOperationsDUnitTest extends CacheTestCase {
  /**
   * The name of our test region.
   */
  public static final String REGION_NAME = "compressedRegion";
  
  /**
   * Test virtual machine number.
   */
  public static final int TEST_VM = 0;
  
  /**
   * A key.
   */
  public static final String KEY_1 = "key1";

  /**
   * Another key.
   */
  public static final String KEY_2 = "key2";

  /**
   * Yet another key.
   */
  public static final String KEY_3 = "key3";  
  
  /**
   * A value.
   */
  public static final String VALUE_1 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aliquam auctor bibendum tempus. Suspendisse potenti. Ut enim neque, mattis et mattis ac, vulputate quis leo. Cras a metus metus, eget cursus ipsum. Aliquam sagittis condimentum massa aliquet rhoncus. Aliquam sed luctus neque. In hac habitasse platea dictumst.";
  
  /**
   * Another value.
   */
  private static final String VALUE_2 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Praesent sit amet lorem consequat est commodo lacinia. Duis tortor sem, facilisis quis tempus in, luctus lacinia metus. Vivamus augue justo, porttitor in vulputate accumsan, adipiscing sit amet sem. Quisque faucibus porta ipsum in pellentesque. Donec malesuada ultrices sapien sit amet tempus. Sed fringilla ipsum at tellus condimentum et hendrerit arcu pretium. Nulla non leo ligula. Etiam commodo tempor ligula non placerat. Vivamus vestibulum varius arcu a varius. Duis sit amet erat imperdiet dui mattis auctor et id orci. Suspendisse non elit augue. Quisque ac orci turpis, nec sollicitudin justo. Sed bibendum justo ut lacus aliquet lacinia et et neque. Proin hendrerit varius mauris vel lacinia. Proin pellentesque lacus vitae nisl euismod bibendum.";

  /**
   * Yet another value.
   */
  private static final String VALUE_3 = "In ut nisi nisi, eu malesuada mauris. Vestibulum nec tellus felis. Pellentesque mauris ligula, pretium nec consequat ut, adipiscing non lorem. Vivamus pulvinar viverra nisl, sit amet vestibulum tellus lobortis in. Pellentesque blandit ipsum sed neque rhoncus eu tristique risus porttitor. Vivamus molestie dapibus mi in lacinia. Suspendisse bibendum, purus at gravida accumsan, libero turpis elementum leo, eget posuere purus nibh ac dolor.";
  
  /**
   * A map of key, value pairs.
   */
  private static Map<String,String> putAllMap = new HashMap<String,String>();
  
  /**
   * A map of key, value pairs.
   */
  private static Map<String,Object> putAllMap2 = new HashMap<String,Object>();

  /**
   * A map of key, value pairs.
   */
  private static Map<String,byte[]> putAllMap3 = new HashMap<String,byte[]>();

  /**
   * A collection of keys.
   */
  private static Collection<String> getAllCollection =  new HashSet<String>();

  /**
   * Populates the put all map and key collection.
   */
  static {
    putAllMap.put(KEY_1, VALUE_1);
    putAllMap.put(KEY_2, VALUE_2);
    putAllMap.put(KEY_3, VALUE_3);
    
    putAllMap2.put(KEY_1, CachedDeserializableFactory.create(EntryEventImpl.serialize(VALUE_1)));
    putAllMap2.put(KEY_2, CachedDeserializableFactory.create(EntryEventImpl.serialize(VALUE_2)));
    putAllMap2.put(KEY_3, CachedDeserializableFactory.create(EntryEventImpl.serialize(VALUE_3)));

    putAllMap3.put(KEY_1, VALUE_1.getBytes());
    putAllMap3.put(KEY_2, VALUE_2.getBytes());
    putAllMap3.put(KEY_3, VALUE_3.getBytes());

    getAllCollection.add(KEY_1);
    getAllCollection.add(KEY_2);
    getAllCollection.add(KEY_3);
  }
  
  /**
   * Creates a new CompressionRegionOperationsDUnitTest.
   * @param name a test name.
   */
  public CompressionRegionOperationsDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    createRegion();
  }
  
  protected void createRegion() {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();;
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    createCompressedRegionOnVm(getVM(TEST_VM), REGION_NAME, compressor);
  }
  
  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    Error error = null;
    Exception exception = null;
    
    try {
      preTearDownCompressionRegionOperationsDUnitTest();
    } catch (Error e) {
      error = e;
    } catch (Exception e) {
      exception = e;
    }
    
    try {
      SnappyCompressor.getDefaultInstance();
      cleanup(getVM(TEST_VM));
    } catch (Throwable t) {
      // Not a supported OS
    }
    
    if (error != null) {
      throw error;
    }
    if (exception != null) {
      throw exception;
    }
  }
  
  protected void preTearDownCompressionRegionOperationsDUnitTest() throws Exception {
  }

  /**
   * Invokes basic get/put operations tests on the test vm.
   */
  public void testGetPutOperations() {
    testGetPutOperationsOnVM(getVM(TEST_VM));
  }
  
  /**
   * Tests the following operations on a region with compression enabled:
   * 
   * <ul>
   * <li>{@link Region#put(Object, Object)}</li>
   * <li>{@link Region#putAll(Map)}</li>
   * <li>{@link Region#putIfAbsent(Object, Object)}</li>
   * <li>{@link Region#get(Object)}</li>
   * <li>{@link Region#getAll(Collection)}</li>
   * </ul>
   * 
   * @param vm a test virtual machine.
   */
  private void testGetPutOperationsOnVM(final VM vm) {
    try {
      SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region<String,String> region = getCache().getRegion(REGION_NAME);        
        String oldValue = (String) region.put(KEY_1, VALUE_1);
        assertNull(oldValue);
        
        oldValue = region.get(KEY_1);
        assertEquals(VALUE_1,oldValue);
        
        oldValue = region.put(KEY_1, VALUE_2);
        if(null != oldValue) {
          assertEquals(VALUE_1,oldValue);          
        }
        
        oldValue = region.get(KEY_1);
        assertEquals(VALUE_2,oldValue);
        
        oldValue = region.putIfAbsent(KEY_1, VALUE_3);
        assertEquals(VALUE_2,oldValue);
        
        region.putAll(putAllMap);
        
        oldValue = region.get(KEY_1);
        assertEquals(VALUE_1,oldValue);
        
        oldValue = region.get(KEY_2);
        assertEquals(VALUE_2,oldValue);
        
        oldValue = region.get(KEY_3);
        assertEquals(VALUE_3,oldValue);
        
        Map<String,String> getAllMap = region.getAll(getAllCollection);
        assertTrue(getAllMap.containsValue(VALUE_1));
        assertTrue(getAllMap.containsValue(VALUE_2));
        assertTrue(getAllMap.containsValue(VALUE_3));
      }      
    });
  }
  
  /**
   * Invokes key, value operations using the test VM.
   */
  public void testKeysAndValuesOperations() {
    testKeysAndValuesOperationsOnVM(getVM(TEST_VM));
  }
  
  /**
   * Tests the following region key, value operations:
   * 
   * <ul>
   * <li>{@link Region#invalidate(Object)}</li>
   * <li>{@link Region#containsKey(Object)}</li>
   * <li>{@link Region#containsValue(Object)}</li>
   * <li>{@link Region#destroy(Object)}</li>
   * <li>{@link Region#remove(Object)}</li>
   * <li>{@link Region#remove(Object, Object)}</li>
   * <li>{@link Region#replace(Object, Object)}</li>
   * <li>{@link Region#replace(Object, Object, Object)}</li>
   * <li>{@link Region#values()}</li>
   * <li>{@link Region#keySet()}</li>
   * </ul>
   * 
   * @param vm a test virtual machine.
   */
  private void testKeysAndValuesOperationsOnVM(final VM vm) {
    try {
      SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    vm.invoke(new SerializableRunnable() {        
      @Override
      public void run() {
        Region<String,String> region = getCache().getRegion(REGION_NAME);
        
        String oldValue = region.put(KEY_1, VALUE_1);
        assertNull(oldValue);
        
        oldValue = region.get(KEY_1);
        assertEquals(VALUE_1,oldValue);
        
        region.invalidate(KEY_1);
        assertNull(region.get(KEY_1));
        
        oldValue = region.put(KEY_1, VALUE_1);
        assertNull(oldValue);
        
        oldValue = region.get(KEY_1);
        assertEquals(VALUE_1,oldValue);
        
        assertTrue(region.containsKey(KEY_1));
        assertTrue(region.containsValue(VALUE_1));
        
        region.destroy(KEY_1);
        assertNull(region.get(KEY_1));
        
        oldValue = region.put(KEY_1, VALUE_1);
        assertNull(oldValue);

        oldValue = region.get(KEY_1);
        assertEquals(VALUE_1,oldValue);
        
        oldValue = region.remove(KEY_1);
        assertEquals(VALUE_1,oldValue);
        
        oldValue = region.put(KEY_1, VALUE_1);
        assertNull(oldValue);

        assertTrue(region.remove(KEY_1, VALUE_1));
        
        oldValue = region.put(KEY_1, VALUE_1);
        assertNull(oldValue);
        
        oldValue = region.replace(KEY_1, VALUE_2);
        assertEquals(VALUE_1,oldValue);
        
        oldValue = region.get(KEY_1);
        assertEquals(VALUE_2,oldValue);
        
        assertTrue(region.replace(KEY_1, VALUE_2, VALUE_3));          
        assertTrue(region.values().contains(VALUE_3));
        
        assertTrue(region.keySet().contains(KEY_1));
      }
    });
  }
  
  /**
   * Tests compressed put/get region operations using CachedDeserializable values.
   * @see CompressionRegionOperationsDUnitTest#testGetPutOperations()
   */
  public void testGetPutOperationsWithCachedDeserializable() {
    testGetPutOperationsWithCachedDeserializableOnVM(getVM(TEST_VM));
  }
  
  /**
   * Tests the following operations on a region with compression enabled
   * using CachedDeserializable values:
   * 
   * <ul>
   * <li>{@link Region#put(Object, Object)}</li>
   * <li>{@link Region#putAll(Map)}</li>
   * <li>{@link Region#putIfAbsent(Object, Object)}</li>
   * <li>{@link Region#get(Object)}</li>
   * <li>{@link Region#getAll(Collection)}</li>
   * </ul>
   * 
   * @param vm a test virtual machine.
   */
  private void testGetPutOperationsWithCachedDeserializableOnVM(final VM vm) {
    try {
      SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region<String,Object> region = getCache().getRegion(REGION_NAME);        
        String oldValue = (String) region.put(KEY_1, CachedDeserializableFactory.create(EntryEventImpl.serialize(VALUE_1)));
        assertNull(oldValue);
        
        oldValue = (String) region.get(KEY_1);
        assertEquals(VALUE_1,oldValue);
        
        oldValue = (String) region.put(KEY_1, CachedDeserializableFactory.create(EntryEventImpl.serialize(VALUE_2)));
        if(null != oldValue) {
          assertEquals(VALUE_1,oldValue);          
        }
        
        oldValue = (String) region.get(KEY_1);
        assertEquals(VALUE_2,oldValue);
        
        oldValue = (String) region.putIfAbsent(KEY_1, CachedDeserializableFactory.create(EntryEventImpl.serialize(VALUE_3)));
        assertEquals(VALUE_2,oldValue);
        
        region.putAll(putAllMap2);
        
        oldValue = (String) region.get(KEY_1);
        assertEquals(VALUE_1,oldValue);
        
        oldValue = (String) region.get(KEY_2);
        assertEquals(VALUE_2,oldValue);
        
        oldValue = (String )region.get(KEY_3);
        assertEquals(VALUE_3,oldValue);
        
        Map<String,Object> getAllMap = region.getAll(getAllCollection);
        assertTrue(getAllMap.containsValue(VALUE_1));
        assertTrue(getAllMap.containsValue(VALUE_2));
        assertTrue(getAllMap.containsValue(VALUE_3));
      }      
    }); 
  }
  
  /**
   * Tests compressed put/get region operations using byte[] values.
   * @see CompressionRegionOperationsDUnitTest#testGetPutOperations()
   */
  public void testGetPutOperationsWithByteArrays() {
    testGetPutOperationsWithByteArraysOnVM(getVM(TEST_VM));
  }
  
  /**
   * Tests the following operations on a region with compression enabled
   * using byte[] values:
   * 
   * <ul>
   * <li>{@link Region#put(Object, Object)}</li>
   * <li>{@link Region#putAll(Map)}</li>
   * <li>{@link Region#putIfAbsent(Object, Object)}</li>
   * <li>{@link Region#get(Object)}</li>
   * <li>{@link Region#getAll(Collection)}</li>
   * </ul>
   * 
   * @param vm a test virtual machine.
   */
  private void testGetPutOperationsWithByteArraysOnVM(final VM vm) {
    try {
      SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region<String,byte[]> region = getCache().getRegion(REGION_NAME);        
        byte[] oldValue = region.put(KEY_1, VALUE_1.getBytes());
        assertNull(oldValue);
        
        oldValue = region.get(KEY_1);
        assertEquals(VALUE_1,new String(oldValue));
        
        oldValue = region.put(KEY_1, VALUE_2.getBytes());
        if(null != oldValue) {
          assertEquals(VALUE_1,new String(oldValue));          
        }
        
        oldValue = region.get(KEY_1);
        assertEquals(VALUE_2,new String(oldValue));
        
        oldValue = region.putIfAbsent(KEY_1, VALUE_3.getBytes());
        assertEquals(VALUE_2,new String(oldValue));
        
        region.putAll(putAllMap3);
        
        oldValue = region.get(KEY_1);
        assertEquals(VALUE_1,new String(oldValue));
        
        oldValue = region.get(KEY_2);
        assertEquals(VALUE_2,new String(oldValue));
        
        oldValue = region.get(KEY_3);
        assertEquals(VALUE_3,new String(oldValue));
        
        Map<String,byte[]> getAllMap = region.getAll(getAllCollection);
        oldValue = getAllMap.get(KEY_1);
        assertEquals(VALUE_1,new String(oldValue));

        oldValue = getAllMap.get(KEY_2);
        assertEquals(VALUE_2,new String(oldValue));

        oldValue = getAllMap.get(KEY_3);
        assertEquals(VALUE_3,new String(oldValue));
      }      
    }); 
  }

  /**
   * Returns the VM for a given identifier.
   * @param vm a virtual machine identifier.
   */
  protected VM getVM(int vm) {
    return Host.getHost(0).getVM(vm);
  }

  /**
   * Removes created regions from a VM.
   * @param vm the virtual machine to cleanup.
   */
  private void cleanup(final VM vm) {
    vm.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        getCache().getRegion(REGION_NAME).destroyRegion();        
      }
    });        
  }

  /**
   * Creates a region and assigns a compressor.
   * 
   * @param vm
   *          a virtual machine to create the region on.
   * @param name
   *          a region name.
   * @param compressor
   *          a compressor.
   * @return true if successfully created, otherwise false.
   */
  private boolean createCompressedRegionOnVm(final VM vm, final String name,
      final Compressor compressor) {
    return createCompressedRegionOnVm(vm, name, compressor, false);
  }
  protected boolean createCompressedRegionOnVm(final VM vm, final String name, final Compressor compressor, final boolean offHeap) {
    return (Boolean) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          createRegion(name, compressor, offHeap);
        } catch (IllegalStateException e) {
          return Boolean.FALSE;
        }

        return Boolean.TRUE;
      }
    });
  }

  /**
   * Creates a region and assigns a compressor.
   * 
   * @param name
   *          a region name.
   * @param compressor
   *          a compressor.
   */
  private Region createRegion(String name, Compressor compressor, boolean offHeap) {
    return getCache().<String,String>createRegionFactory().setDataPolicy(DataPolicy.REPLICATE).setCloningEnabled(true).setCompressor(compressor).setOffHeap(offHeap).create(name);
  }
}
