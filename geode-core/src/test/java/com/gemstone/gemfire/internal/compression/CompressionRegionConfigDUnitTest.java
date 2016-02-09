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

import java.io.IOException;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.dunit.standalone.DUnitLauncher;

/**
 * Sanity checks on a number of basic cluster configurations with compression turned on.
 * 
 * @author rholmes
 */
public class CompressionRegionConfigDUnitTest extends CacheTestCase {
  /**
   * The name of our test region.
   */
  public static final String REGION_NAME = "compressedRegion";
  
  /**
   * Name of the common disk store used by all tests.
   */
  public static final String DISK_STORE = "diskStore";
  
  /**
   * A key.
   */
  public static final String KEY_1 = "key1";

  /**
   * A value.
   */
  public static final String VALUE_1 = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aliquam auctor bibendum tempus. Suspendisse potenti. Ut enim neque, mattis et mattis ac, vulputate quis leo. Cras a metus metus, eget cursus ipsum. Aliquam sagittis condimentum massa aliquet rhoncus. Aliquam sed luctus neque. In hac habitasse platea dictumst.";
  
  /**
   * Creates a new CompressionRegionOperationsDUnitTest.
   * @param name a test name.
   */
  public CompressionRegionConfigDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  
  /**
   * Sanity check using two peers sharing a replicated region.
   * @throws Exception
   */
  public void testReplicateRegion() throws Exception {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    assertTrue(createCompressedRegionOnVm(getVM(0), REGION_NAME, DataPolicy.REPLICATE, compressor));
    assertTrue(createCompressedRegionOnVm(getVM(1), REGION_NAME, DataPolicy.REPLICATE, compressor));
    assertNull(putUsingVM(getVM(0),KEY_1,VALUE_1));
    waitOnPut(getVM(1),KEY_1);
    assertEquals(VALUE_1,getUsingVM(getVM(1), KEY_1));
    cleanup(getVM(0));          
  }
  
  /**
   * Sanity check for two peers sharing a persisted replicated region.
   * @throws Exception
   */
  public void testReplicatePersistentRegion() throws Exception {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    assertTrue(createCompressedRegionOnVm(getVM(0), REGION_NAME, DataPolicy.PERSISTENT_REPLICATE, compressor, DISK_STORE));
    assertTrue(createCompressedRegionOnVm(getVM(1), REGION_NAME, DataPolicy.PERSISTENT_REPLICATE, compressor));
    assertNull(putUsingVM(getVM(0),KEY_1,VALUE_1));
    waitOnPut(getVM(1),KEY_1);  
    flushDiskStoreOnVM(getVM(0), DISK_STORE);     
    closeRegionOnVM(getVM(1), REGION_NAME);
    assertTrue(createCompressedRegionOnVm(getVM(1), REGION_NAME, DataPolicy.PERSISTENT_REPLICATE, compressor));
    assertEquals(VALUE_1,getUsingVM(getVM(1), KEY_1));
    cleanup(getVM(0));
  }

  /**
   * Sanity check for two peers hosting a partitioned region.
   */
  public void testPartitionedRegion() {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    assertTrue(createCompressedRegionOnVm(getVM(0), REGION_NAME, DataPolicy.PARTITION, compressor));
    assertTrue(createCompressedRegionOnVm(getVM(1), REGION_NAME, DataPolicy.PARTITION, compressor));
    assertNull(putUsingVM(getVM(0),KEY_1,VALUE_1));
    waitOnPut(getVM(1),KEY_1);
    assertEquals(VALUE_1,getUsingVM(getVM(1), KEY_1));
    cleanup(getVM(0));      
  }
  
  /**
   * Sanity check for two peers hosting a persistent partitioned region.
   */
  public void testPartitionedPersistentRegion() {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    assertTrue(createCompressedRegionOnVm(getVM(0), REGION_NAME, DataPolicy.PERSISTENT_PARTITION, compressor, DISK_STORE));
    assertTrue(createCompressedRegionOnVm(getVM(1), REGION_NAME, DataPolicy.PERSISTENT_PARTITION, compressor));
    assertNull(putUsingVM(getVM(0),KEY_1,VALUE_1));
    waitOnPut(getVM(1),KEY_1);
    flushDiskStoreOnVM(getVM(0), DISK_STORE);
    closeRegionOnVM(getVM(1), REGION_NAME);
    assertTrue(createCompressedRegionOnVm(getVM(1), REGION_NAME, DataPolicy.PERSISTENT_PARTITION, compressor));
    assertEquals(VALUE_1,getUsingVM(getVM(1), KEY_1));
    cleanup(getVM(0));
  }
  
  /**
   * Sanity check for a non caching client and a cache server.
   */
  public void testClientProxyRegion() {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    assertTrue(createCompressedServerRegionOnVm(getVM(0), REGION_NAME, DataPolicy.REPLICATE, compressor));
    assertTrue(createCompressedClientRegionOnVm(getVM(1), REGION_NAME, compressor, ClientRegionShortcut.PROXY));
    assertNull(putUsingClientVM(getVM(1),KEY_1,VALUE_1));
    assertEquals(VALUE_1,getUsingClientVM(getVM(1), KEY_1));
    assertEquals(VALUE_1,getUsingVM(getVM(0),KEY_1));
    cleanupClient(getVM(1));
    cleanup(getVM(0));
  }
  
  /**
   * Sanity check for a caching client and a cache server.
   */
  public void testCachingClientProxyRegion() {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    assertTrue(createCompressedServerRegionOnVm(getVM(0), REGION_NAME, DataPolicy.REPLICATE, compressor));
    assertTrue(createCompressedClientRegionOnVm(getVM(1), REGION_NAME, compressor, ClientRegionShortcut.CACHING_PROXY));
    assertNull(putUsingClientVM(getVM(1),KEY_1,VALUE_1));
    assertEquals(VALUE_1,getUsingClientVM(getVM(1), KEY_1));
    assertEquals(VALUE_1,getUsingVM(getVM(0),KEY_1));      
    cleanupClient(getVM(1));
    cleanup(getVM(0));
  }

  /**
   * closes a region on a virtual machine.
   * @param vm a virtual machine.
   * @param region the region to close.
   */
  private void closeRegionOnVM(final VM vm, final String region) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        getCache().getRegion(region).close();
      }      
    });
  }
  
  /**
   * Flushes a disk store on a vm causing region entries to be written to disk.
   * @param vm the virtual machine to perform the flush on.
   * @param diskStore the disk store to flush.
   */
  private void flushDiskStoreOnVM(final VM vm, final String diskStore) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        getCache().findDiskStore(diskStore).flush();
      }      
    });
  }
  
  /**
   * Returns when a put has been replicated to a peer.
   * @param vm a peer.
   * @param key the key to wait on.
   */
  private void waitOnPut(final VM vm, final String key) {
    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return (getUsingVM(vm, key) != null);
      }

      @Override
      public String description() {
        return "Waiting on " + key + " to replicate.";
      }        
    },2000,500,true);          
  }
  
  /**
   * Performs a put operation on a client virtual machine.
   * @param vm a client.
   * @param key the key to put.
   * @param value the value to put.
   * @return the old value.
   */
  private String putUsingClientVM(final VM vm,final String key,final String value) {
    return (String) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCache cache = getClientCache(getClientCacheFactory(getLocatorPort()));
        Region<String,String> region = cache.getRegion(REGION_NAME);
        assertNotNull(region);
        return region.put(key, value);
      }      
    });
  }

  /**
   * Performs a put operation on a peer.
   * @param vm a peer.
   * @param key the key to put.
   * @param value the value to put.
   * @return the old value.
   */
  private String putUsingVM(final VM vm,final String key,final String value) {
    return (String) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<String,String> region = getCache().getRegion(REGION_NAME);
        assertNotNull(region);
        return region.put(key, value);
      }      
    });
  }

  /**
   * Performs a get operation on a client.
   * @param vm a client.
   * @param key a region entry key.
   * @return the value.
   */
  private String getUsingClientVM(final VM vm,final String key) {
    return (String) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClientCache cache = getClientCache(getClientCacheFactory(getLocatorPort()));
        Region<String,String> region = cache.getRegion(REGION_NAME);
        assertNotNull(region);
        return region.get(key);
      }      
    });    
  }  
  
  /**
   * Performs a get operation on a peer.
   * @param vm the peer.
   * @param key a region entry key.
   * @return the value.
   */
  private String getUsingVM(final VM vm,final String key) {
    return (String) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Region<String,String> region = getCache().getRegion(REGION_NAME);
        assertNotNull(region);
        return region.get(key);
      }      
    });    
  }
  
  /**
   * Returns the VM for a given identifier.
   * @param vm a virtual machine identifier.
   */
  private VM getVM(int vm) {
    return Host.getHost(0).getVM(vm);
  }

  /**
   * Closes opened regions on a client.
   * @param vm a client.
   */
  private void cleanupClient(final VM vm) {
    vm.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        getClientCache(getClientCacheFactory(getLocatorPort())).getRegion(REGION_NAME).close();        
      }
    });            
  }
  
  /**
   * Removes created regions from a VM.
   * @param vm the virtual machine to cleanup.
   */
  private void cleanup(final VM vm) {
    vm.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        Region<String,String> region = getCache().getRegion(REGION_NAME);         
        assertNotNull(region);
        region.destroyRegion();        
      }
    });        
  }

  /**
   * Creates a region and assigns a compressor..
   * @param vm a virtual machine to create the region on.
   * @param name a region name.
   * @param compressor a compressor.
   * @return true if successfully created, otherwise false.
   */
  private boolean createCompressedServerRegionOnVm(final VM vm,final String name,final DataPolicy dataPolicy,final Compressor compressor) {
    return (Boolean) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          assertNotNull(createServerRegion(name,dataPolicy,compressor));
        } catch(Exception e) {
          LogWriterUtils.getLogWriter().error("Could not create the compressed region", e);
          return Boolean.FALSE;
        }
        
        return Boolean.TRUE;
      }      
    });
  }

  /**
   * Creates a region and assigns a compressor.
   * @param vm a virtual machine to create the region on.
   * @param name a region name.
   * @param compressor a compressor.
   * @return true if successfully created, otherwise false.
   */
  private boolean createCompressedRegionOnVm(final VM vm,final String name,final DataPolicy dataPolicy,final Compressor compressor) {
    return (Boolean) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          assertNotNull(createRegion(name,dataPolicy,compressor));
        } catch(Exception e) {
          LogWriterUtils.getLogWriter().error("Could not create the compressed region", e);
          return Boolean.FALSE;
        }
        
        return Boolean.TRUE;
      }      
    });
  }

  /**
   * Creates a region and assigns a compressor.
   * @param vm a virtual machine to create the region on.
   * @param name a region name.
   * @param compressor a compressor.
   * @return true if successfully created, otherwise false.
   */
  private boolean createCompressedRegionOnVm(final VM vm,final String name,final DataPolicy dataPolicy,final Compressor compressor,final String diskStoreName) {
    return (Boolean) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          assertNotNull(createRegion(name,dataPolicy,compressor,diskStoreName));
        } catch(Exception e) {
          LogWriterUtils.getLogWriter().error("Could not create the compressed region", e);
          return Boolean.FALSE;
        }
        
        return Boolean.TRUE;
      }      
    });
  }

  /**
   * Creates a compressed region on a client.
   * @param vm the client.
   * @param name the region.
   * @param compressor a compressor.
   * @param shortcut type of client.
   * @return true if created, false otherwise.
   */
  private boolean createCompressedClientRegionOnVm(final VM vm, final String name, final Compressor compressor, final ClientRegionShortcut shortcut) {
    return (Boolean) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          assertNotNull(createClientRegion(name,compressor,shortcut));
        } catch(Exception e) {
          LogWriterUtils.getLogWriter().error("Could not create the compressed region", e);
          return Boolean.FALSE;
        }
        
        return Boolean.TRUE;
      }      
    });
  }
  
  /**
   * Creates a compressed region for a client.
   * @param name a region.
   * @param compressor a compressor.
   * @param shortcut the type of client.
   * @return the newly created region.
   */
  private Region<String,String> createClientRegion(String name,Compressor compressor,ClientRegionShortcut shortcut) {
    ClientCacheFactory factory = getClientCacheFactory(getLocatorPort());
    return getClientCache(factory).<String,String>createClientRegionFactory(shortcut).setCloningEnabled(true).setCompressor(compressor).create(name);
  }
  
  /**
   * Creates a region and assigns a compressor.
   * @param name the region name.
   * @param dataPolicy the type of peer.
   * @param compressor a compressor.
   * @return the newly created region.
   */
  private Region<String,String> createRegion(String name,DataPolicy dataPolicy,Compressor compressor) {    
    return getCache().<String,String>createRegionFactory().setDataPolicy(dataPolicy).setCloningEnabled(true).setCompressor(compressor).create(name);
  }

  /**
   * Creates a compressed region and adds a cache server to a peer.
   * @param name the region name.
   * @param dataPolicy the type of peer.
   * @param compressor a compressor
   * @return the newly created region.
   * @throws IOException a problem occurred while created the cache server.
   */
  private Region<String,String> createServerRegion(String name,DataPolicy dataPolicy,Compressor compressor) throws IOException {
    Region<String,String> region = getCache().<String,String>createRegionFactory().setDataPolicy(dataPolicy).setCloningEnabled(true).setCompressor(compressor).create(name);
    CacheServer server = getCache().addCacheServer();
    server.setPort(0);
    server.start();
    
    return region;
  }
  
  /**
   * Creates a region and assigns a compressor.
   * @param name the region name.
   * @param dataPolicy the type of peer.
   * @param compressor a compressor.
   * @return the newly created region.
   */
  private Region<String,String> createRegion(String name,DataPolicy dataPolicy,Compressor compressor,String diskStoreName) {
    getCache().createDiskStoreFactory().create(diskStoreName);
    return getCache().<String,String>createRegionFactory().setDataPolicy(dataPolicy).setDiskStoreName(diskStoreName).setCloningEnabled(true).setCompressor(compressor).create(name);
  }
  
  /**
   * Creates a new ClientCacheFactory.
   * @param dunitLocatorPort a locator port.
   * @return the newly created ClientCacheFactory.
   */
  private ClientCacheFactory getClientCacheFactory(int dunitLocatorPort) {
    return new ClientCacheFactory().addPoolLocator("localhost", dunitLocatorPort).setPoolSubscriptionEnabled(true);
  }
  
  /**
   * Returns the locator port.
   */
  private int getLocatorPort() {
    // Running from eclipse
    if(DUnitLauncher.isLaunched()) {
      String locatorString = DUnitLauncher.getLocatorString();
      int index = locatorString.indexOf("[");
      return Integer.parseInt(locatorString.substring(index+1, locatorString.length()-1));
    } 
    // Running in hydra
    else {
      return DistributedTestUtils.getDUnitLocatorPort();
    }
  }
}
