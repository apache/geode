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

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests that the compressor region attribute is properly set or rejected by a RegionFactory.
 * @author rholmes
 */
public class CompressionRegionFactoryDUnitTest extends CacheTestCase {
  /**
   * Compressed region name.
   */
  protected static final String COMPRESSED_REGION_NAME = "compressedRegion";
  
  /**
   * A valid compressor.
   */
  protected static final Compressor compressor = SnappyCompressor.getDefaultInstance();
  
  /**
   * Our test vm.
   */
  protected static final int TEST_VM = 0;
  
  /**
   * Creates a new CompressionRegionFactoryDUnitTest.
   * @param name test name.
   */
  public CompressionRegionFactoryDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  
  /**
   * Asserts that a region is created when a valid compressor is used.
   * Asserts that the region attributes contain the correct compressor value. 
   */
  public void testRegionFactoryCompressor() {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    assertTrue(createCompressedRegionOnVm(getVM(TEST_VM), COMPRESSED_REGION_NAME, compressor));
    assertCompressor(getVM(TEST_VM), COMPRESSED_REGION_NAME, compressor);
    cleanup(getVM(TEST_VM));
  }
  
  /**
   * Returns the VM for a given identifier.
   * @param vm a virtual machine identifier.
   */
  private VM getVM(int vm) {
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
        getCache().getRegion(COMPRESSED_REGION_NAME).destroyRegion();        
      }
    });        
  }
  
  /**
   * Asserts that a given compressor has been assigned to a region.
   * @param vm the virtual machine to run the assertions on.
   * @param name a region name.
   * @param compressor a compressor.
   */
  private void assertCompressor(final VM vm,final String name,final Compressor compressor) {
    vm.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        Region region = getCache().getRegion(name);
        assertNotNull(region);
        assertNotNull(region.getAttributes().getCompressor());
        assertTrue(compressor.equals(region.getAttributes().getCompressor()));
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
  private boolean createCompressedRegionOnVm(final VM vm,final String name,final Compressor compressor) {
    return (Boolean) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          createRegion(name,compressor);
        } catch(IllegalStateException e) {
          return Boolean.FALSE;
        }
        
        return Boolean.TRUE;
      }      
    });
  }

  /**
   * Creates a region and assigns a compressor.
   * @param name a region name.
   * @param compressor a compressor.
   */
  private Region createRegion(String name,Compressor compressor) {
    return getCache().<String,String>createRegionFactory().setDataPolicy(DataPolicy.REPLICATE).setCloningEnabled(true).setCompressor(compressor).create(name);
  }
}
