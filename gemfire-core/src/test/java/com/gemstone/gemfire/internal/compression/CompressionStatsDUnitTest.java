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

import java.io.Serializable;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests compression statistics.
 * @author rholmes
 * @since 8.0
 */
public class CompressionStatsDUnitTest extends CacheTestCase {
  /**
   * The name of our test region.
   */
  public static final String REGION_NAME = "compressedRegion";
  
  /**
   * The name of our test region.
   */
  public static final String REGION_NAME_2 = "compressedRegion2";
  
  /**
   * Test virtual machine number.
   */
  public static final int TEST_VM = 0;
  
  /**
   * Used as the key for put and update ops.
   */
  private static int index = 0;

  private final byte[] PRE_COMPRESS_BYTES = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890".getBytes();
  
  /**
   * Creates a new CompressionStatsDUnitTest.
   * @param name test name.
   */
  public CompressionStatsDUnitTest(String name) {
    super(name);
  }

  /**
   * Asserts that all compression statistics start with zero values.
   * @param vm the virtual machine to gather statistics on.
   */
  private void assertStartingValues(final VM vm) {
    assertEquals(0,getTotalCompressionTimeOnVm(vm, null));
    assertEquals(0,getTotalCompressionTimeOnVm(vm, REGION_NAME));
    assertEquals(0,getTotalCompressionTimeOnVm(vm, REGION_NAME_2));
    
    assertEquals(0,getTotalDecompressionTimeOnVm(vm, null));
    assertEquals(0,getTotalDecompressionTimeOnVm(vm, REGION_NAME));
    assertEquals(0,getTotalDecompressionTimeOnVm(vm, REGION_NAME_2));    
    
    assertEquals(0,getTotalCompressionsOnVm(vm, null));
    assertEquals(0,getTotalCompressionsOnVm(vm, REGION_NAME));
    assertEquals(0,getTotalCompressionsOnVm(vm, REGION_NAME_2));    

    assertEquals(0,getTotalDecompressionsOnVm(vm, null));
    assertEquals(0,getTotalDecompressionsOnVm(vm, REGION_NAME));
    assertEquals(0,getTotalDecompressionsOnVm(vm, REGION_NAME_2));    

    assertEquals(0,getTotalPreCompressedBytesOnVm(vm, null));
    assertEquals(0,getTotalPreCompressedBytesOnVm(vm, REGION_NAME));
    assertEquals(0,getTotalPreCompressedBytesOnVm(vm, REGION_NAME_2));    

    assertEquals(0,getTotalPostCompressedBytesOnVm(vm, null));
    assertEquals(0,getTotalPostCompressedBytesOnVm(vm, REGION_NAME));
    assertEquals(0,getTotalPostCompressedBytesOnVm(vm, REGION_NAME_2));        
  }
  
  /**
   * Performs puts on region one and asserts that the second region's stats are unaffected and that the
   * region stats have rolled up to the cache stats.
   * @param vm the virtual machine to perform puts on.
   * @param stats holder for stat values.
   */
  private void assertRegionOneStats(final VM vm,final CompressionStats stats) {
    doPutsOnVm(vm,REGION_NAME,100);
    
    stats.vmTotalCompressionTime = getTotalCompressionTimeOnVm(vm, null); 
    assertTrue(stats.vmTotalCompressionTime > 0);
    
    stats.region1TotalCompressionTime = getTotalCompressionTimeOnVm(vm, REGION_NAME);  
    assertTrue(stats.region1TotalCompressionTime > 0);
    
    stats.region2TotalCompressionTime = getTotalCompressionTimeOnVm(vm, REGION_NAME_2); 
    assertEquals(0,stats.region2TotalCompressionTime);
    
    stats.vmTotalDecompressionTime = getTotalDecompressionTimeOnVm(vm, null); 
    assertEquals(0,stats.vmTotalDecompressionTime);
    
    stats.region1TotalDecompressionTime = getTotalDecompressionTimeOnVm(vm, REGION_NAME); 
    assertEquals(0,stats.region1TotalDecompressionTime);
    
    stats.region2TotalDecompressionTime = getTotalDecompressionTimeOnVm(vm, REGION_NAME_2); 
    assertEquals(0,stats.region2TotalDecompressionTime);
    
    stats.vmTotalCompressions = getTotalCompressionsOnVm(vm, null);
    assertEquals(100,stats.vmTotalCompressions);
    
    stats.region1TotalCompressions = getTotalCompressionsOnVm(vm, REGION_NAME);
    assertEquals(100,stats.region1TotalCompressions);

    stats.region2TotalCompressions = getTotalCompressionsOnVm(vm, REGION_NAME_2);
    assertEquals(0,stats.region2TotalCompressions);

    stats.vmTotalDecompressions = getTotalDecompressionsOnVm(vm, null);
    assertEquals(0,stats.vmTotalDecompressions);
    
    stats.region1TotalDecompressions = getTotalDecompressionsOnVm(vm, REGION_NAME);
    assertEquals(0,stats.region1TotalDecompressions);

    stats.region2TotalDecompressions = getTotalDecompressionsOnVm(vm, REGION_NAME_2);
    assertEquals(0,stats.region2TotalDecompressions);
    
    stats.vmTotalPreCompressedBytes = getTotalPreCompressedBytesOnVm(vm, null);
    assertEquals(10200,stats.vmTotalPreCompressedBytes);
    
    stats.region1TotalPreCompressedBytes = getTotalPreCompressedBytesOnVm(vm, REGION_NAME);
    assertEquals(10200,stats.region1TotalPreCompressedBytes);
    
    stats.region2TotalPreCompressedBytes = getTotalPreCompressedBytesOnVm(vm, REGION_NAME_2);
    assertEquals(0,stats.region2TotalPreCompressedBytes);

    stats.vmTotalPostCompressedBytes = getTotalPostCompressedBytesOnVm(vm, null);
    assertEquals(5000,stats.vmTotalPostCompressedBytes);
    
    stats.region1TotalPostCompressedBytes = getTotalPostCompressedBytesOnVm(vm, REGION_NAME);
    assertEquals(5000,stats.region1TotalPostCompressedBytes);
    
    stats.region2TotalPostCompressedBytes = getTotalPostCompressedBytesOnVm(vm, REGION_NAME_2);
    assertEquals(0,stats.region2TotalPostCompressedBytes);
  }

  /**
   * Performs puts on the second region and asserts that the compression stats are recorded.
   * @param vm a virtual machine to peform puts on.
   * @param stats a compression stats holder.
   */
  private void assertRegionTwoStats(final VM vm,final CompressionStats stats) {
    doPutsOnVm(vm,REGION_NAME_2,100);
    
    stats.vmTotalCompressionTime = getTotalCompressionTimeOnVm(vm, null); 
    assertTrue(stats.vmTotalCompressionTime > 0);
    
    stats.region1TotalCompressionTime = getTotalCompressionTimeOnVm(vm, REGION_NAME);  
    assertTrue(stats.region1TotalCompressionTime > 0);
    
    stats.region2TotalCompressionTime = getTotalCompressionTimeOnVm(vm, REGION_NAME_2); 
    assertTrue(stats.region2TotalCompressionTime > 0);
    
    stats.vmTotalDecompressionTime = getTotalDecompressionTimeOnVm(vm, null); 
    assertEquals(0,stats.vmTotalDecompressionTime);
    
    stats.region1TotalDecompressionTime = getTotalDecompressionTimeOnVm(vm, REGION_NAME); 
    assertEquals(0,stats.region1TotalDecompressionTime);
    
    stats.region2TotalDecompressionTime = getTotalDecompressionTimeOnVm(vm, REGION_NAME_2); 
    assertEquals(0,stats.region2TotalDecompressionTime);
    
    stats.vmTotalCompressions = getTotalCompressionsOnVm(vm, null);
    assertEquals(200,stats.vmTotalCompressions);
    
    stats.region1TotalCompressions = getTotalCompressionsOnVm(vm, REGION_NAME);
    assertEquals(100,stats.region1TotalCompressions);

    stats.region2TotalCompressions = getTotalCompressionsOnVm(vm, REGION_NAME_2);
    assertEquals(100,stats.region2TotalCompressions);

    stats.vmTotalDecompressions = getTotalDecompressionsOnVm(vm, null);
    assertEquals(0,stats.vmTotalDecompressions);
    
    stats.region1TotalDecompressions = getTotalDecompressionsOnVm(vm, REGION_NAME);
    assertEquals(0,stats.region1TotalDecompressions);

    stats.region2TotalDecompressions = getTotalDecompressionsOnVm(vm, REGION_NAME_2);
    assertEquals(0,stats.region2TotalDecompressions);
    
    stats.vmTotalPreCompressedBytes = getTotalPreCompressedBytesOnVm(vm, null);
    assertEquals(20400,stats.vmTotalPreCompressedBytes);
    
    stats.region1TotalPreCompressedBytes = getTotalPreCompressedBytesOnVm(vm, REGION_NAME);
    assertEquals(10200,stats.region1TotalPreCompressedBytes);
    
    stats.region2TotalPreCompressedBytes = getTotalPreCompressedBytesOnVm(vm, REGION_NAME_2);
    assertEquals(10200,stats.region2TotalPreCompressedBytes);

    stats.vmTotalPostCompressedBytes = getTotalPostCompressedBytesOnVm(vm, null);
    assertEquals(10000,stats.vmTotalPostCompressedBytes);
    
    stats.region1TotalPostCompressedBytes = getTotalPostCompressedBytesOnVm(vm, REGION_NAME);
    assertEquals(5000,stats.region1TotalPostCompressedBytes);
    
    stats.region2TotalPostCompressedBytes = getTotalPostCompressedBytesOnVm(vm, REGION_NAME_2);
    assertEquals(5000,stats.region2TotalPostCompressedBytes);   
  }
  
  private void assertRegionOneStatsAfterGets(final VM vm, final CompressionStats stats) {
    doGetsOnVm(vm, REGION_NAME);

    stats.vmTotalDecompressionTime = getTotalDecompressionTimeOnVm(vm, null); 
    assertTrue(stats.vmTotalDecompressionTime > 0);
    
    stats.region1TotalDecompressionTime = getTotalDecompressionTimeOnVm(vm, REGION_NAME); 
    assertTrue(stats.region1TotalDecompressionTime > 0);
    
    stats.region2TotalDecompressionTime = getTotalDecompressionTimeOnVm(vm, REGION_NAME_2); 
    assertEquals(0,stats.region2TotalDecompressionTime);

    stats.vmTotalDecompressions = getTotalDecompressionsOnVm(vm, null);
    assertEquals(100,stats.vmTotalDecompressions);
    
    stats.region1TotalDecompressions = getTotalDecompressionsOnVm(vm, REGION_NAME);
    assertEquals(100,stats.region1TotalDecompressions);

    stats.region2TotalDecompressions = getTotalDecompressionsOnVm(vm, REGION_NAME_2);
    assertEquals(0,stats.region2TotalDecompressions);

    stats.vmTotalPostCompressedBytes = getTotalPostCompressedBytesOnVm(vm, null);
    assertEquals(10000,stats.vmTotalPostCompressedBytes);  ;
    
    stats.region1TotalPostCompressedBytes = getTotalPostCompressedBytesOnVm(vm, REGION_NAME);
    assertEquals(5000,stats.region1TotalPostCompressedBytes);  
    
    stats.region2TotalPostCompressedBytes = getTotalPostCompressedBytesOnVm(vm, REGION_NAME_2);
    assertEquals(5000,stats.region2TotalPostCompressedBytes);    
  }
  
  /**
   * Asserts that the cache stats are totals of the region one and region two stats.
   * @param stats a compression stats container.
   */
  private void assertStatTotals(final CompressionStats stats) {
    assertEquals(stats.vmTotalPreCompressedBytes,stats.region1TotalPreCompressedBytes + stats.region2TotalPreCompressedBytes);
    assertEquals(stats.vmTotalCompressions,stats.region1TotalCompressions + stats.region2TotalCompressions);
    assertEquals(stats.vmTotalCompressionTime,stats.region1TotalCompressionTime + stats.region2TotalCompressionTime);
    assertEquals(stats.vmTotalPostCompressedBytes,stats.region1TotalPostCompressedBytes + stats.region2TotalPostCompressedBytes);
    assertEquals(stats.vmTotalDecompressions,stats.region1TotalDecompressions + stats.region2TotalDecompressions);
    assertEquals(stats.vmTotalDecompressionTime,stats.region1TotalDecompressionTime + stats.region2TotalDecompressionTime);
  }
  
  /**
   * Asserts that compression stats are functioning properly.
   */
  public void testCompressionStats() {
    VM vm = Host.getHost(0).getVM(TEST_VM);
    
    assertTrue(createCompressedRegionOnVm(vm,REGION_NAME, new StatCompressor()));
    assertTrue(createCompressedRegionOnVm(vm,REGION_NAME_2, new StatCompressor()));
    boolean previousClockStatsValue = enableClockStatsOnVm(vm, true);      

    CompressionStats stats = new CompressionStats();
    
    assertStartingValues(vm);
    assertRegionOneStats(vm,stats);
    assertRegionTwoStats(vm,stats);
    assertRegionOneStatsAfterGets(vm,stats);
    assertStatTotals(stats);
    
    destroyRegionOnVm(vm,REGION_NAME);
    destroyRegionOnVm(vm,REGION_NAME_2);
    enableClockStatsOnVm(vm, previousClockStatsValue);
  }

  private void doGetsOnVm(final VM vm,final String regionName) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region region = getCache().getRegion(regionName);
        assertNotNull(region);
  
        for(int i = 0; i < (index / 2); ++i) {
          assertNotNull(region.get(i));
        }  
      }      
    });    
  }
  
  /**
   * Does put operations on a designated virtual machine.
   * @param vm a virtual machine.
   * @param regionName a region for the puts.
   * @param puts the number of puts to perform.
   */
  private void doPutsOnVm(final VM vm,final String regionName,final int puts) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region region = getCache().getRegion(regionName);
        assertNotNull(region);
        for(int i = 0; i < puts; ++i) {
          region.put(index++,PRE_COMPRESS_BYTES);
        }       
      }      
    });
  }
 
  /**
   * Returns the total number of post-compressed bytes stat for a virtual machine.
   * @param vm a virtual machine.
   * @param regionName designates a region on which to collect the number of decompressed bytes.  Null indicates the whole cache.
   */
  private long getTotalPostCompressedBytesOnVm(final VM vm,final String regionName) {
    return (Long) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        if(null == regionName) {
          return getTotalPostCompressedBytes();
        } else {
          return getTotalPostCompressedBytes(regionName);
        }
      }      
    });
  }

  /**
   * Returns the the number of post-compressed bytes stat for the cache.
   */
  private long getTotalPostCompressedBytes() {
    GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    return cache.getCachePerfStats().getTotalPostCompressedBytes();
  }
  
  /**
   * Returns the number of post-decompressed bytes stat for a region.
   * @param regionName a region.
   */
  private long getTotalPostCompressedBytes(String regionName) {
    LocalRegion region =  (LocalRegion) getCache().getRegion(regionName);
    assertNotNull(region);

    return region.getCachePerfStats().getTotalPostCompressedBytes();
  }

  /**
   * Returns the number of pre-compressed bytes stat on a virtual machine.
   * @param vm a virtual machine.
   * @param regionName a region.  Indicates whole cache if null.
   */
  private long getTotalPreCompressedBytesOnVm(final VM vm,final String regionName) {
    return (Long) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        if(null == regionName) {
          return getTotalPreCompressedBytes();
        } else {
          return getTotalPreCompressedBytes(regionName);
        }
      }      
    });
  }
  
  /**
   * Returns the number of compressed bytes stat for the cache.
   */
  private long getTotalPreCompressedBytes() {
    GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    return cache.getCachePerfStats().getTotalPreCompressedBytes();
  }
  
  /**
   * Returns the number of pre-compressed bytes stat for a region.
   * @param regionName a region.
   */
  private long getTotalPreCompressedBytes(String regionName) {
    LocalRegion region =  (LocalRegion) getCache().getRegion(regionName);
    assertNotNull(region);

    return region.getCachePerfStats().getTotalPreCompressedBytes();
  }

  /**
   * Returns the number of decompressions stat for a virtual machine.
   * @param vm a virtual machine.
   * @param regionName a region.  Indicates the whole cache when null.
   */
  private long getTotalDecompressionsOnVm(final VM vm,final String regionName) {
    return (Long) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        if(null == regionName) {
          return getTotalDecompressions();
        } else {
          return getTotalDecompressions(regionName);
        }
      }      
    });
  }
  
  /**
   * Returns the number of decompressions stat for the cache.
   */
  private long getTotalDecompressions() {
    GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    return cache.getCachePerfStats().getTotalDecompressions();
  }
  
  /**
   * Returns the number of decompressions stat for a region.
   * @param regionName a region.
   */
  private long getTotalDecompressions(String regionName) {
    LocalRegion region =  (LocalRegion) getCache().getRegion(regionName);
    assertNotNull(region);

    return region.getCachePerfStats().getTotalDecompressions();
  }

  /**
   * Returns the total number of compressions stat for a virtual machine.
   * @param vm a virtual machine.
   * @param regionName a region.  Indicates the entire cache when null.
   */
  private long getTotalCompressionsOnVm(final VM vm,final String regionName) {
    return (Long) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        if(null == regionName) {
          return getTotalCompressions();
        } else {
          return getTotalCompressions(regionName);
        }
      }      
    });
  }
  
  /**
   * Returns the total number of compressions stat for the cache.
   */
  private long getTotalCompressions() {
    GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    return cache.getCachePerfStats().getTotalCompressions();
  }

  /**
   * Returns the total number of compressions stat for a region.
   * @param regionName a region.
   */
  private long getTotalCompressions(String regionName) {
    LocalRegion region =  (LocalRegion) getCache().getRegion(regionName);
    assertNotNull(region);

    return region.getCachePerfStats().getTotalCompressions();
  }

  /**
   * Returns the total decompression time stat for a virtual machine.
   * @param vm a virtual machine.
   * @param regionName a region.  Indicates the whole cache when null.
   */
  private long getTotalDecompressionTimeOnVm(final VM vm,final String regionName) {
    return (Long) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        if(null == regionName) {
          return getTotalDecompressionTime();
        } else {
          return getTotalDecompressionTime(regionName);
        }
      }      
    });
  }
  
  /**
   * Returns the total decompression time stat for the cache.
   */
  private long getTotalDecompressionTime() {
    GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    return cache.getCachePerfStats().getTotalDecompressionTime();
  }
  
  /**
   * Returns the total decompression time stat for a region.
   * @param regionName a region.
   */
  private long getTotalDecompressionTime(String regionName) {
    LocalRegion region =  (LocalRegion) getCache().getRegion(regionName);
    assertNotNull(region);

    return region.getCachePerfStats().getTotalDecompressionTime();
  }

  /**
   * Returns the total compression time stat for a virtual machine.
   * @param vm a virtual machine.
   * @param regionName a region.
   */
  private long getTotalCompressionTimeOnVm(final VM vm,final String regionName) {
    return (Long) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        if(null == regionName) {
          return getTotalCompressionTime();
        } else {
          return getTotalCompressionTime(regionName);
        }
      }      
    });
  }

  /**
   * Returns the total compression time stat for the cache.
   */
  private long getTotalCompressionTime() {
    GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    return cache.getCachePerfStats().getTotalCompressionTime();
  }

  /**
   * Returns the total compression time stat for a region.
   * @param regionName a region.
   */
  private long getTotalCompressionTime(String regionName) {
    LocalRegion region =  (LocalRegion) getCache().getRegion(regionName);
    assertNotNull(region);

    return region.getCachePerfStats().getTotalCompressionTime();
  }
  
  /**
   * Destroys a region.
   * @param vm a virtual machine.
   * @param regionName the region to destroy.
   */
  private void destroyRegionOnVm(final VM vm,final String regionName) {
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        destroyRegion(regionName);
      }      
    });
  }
  
  /**
   * Destroys a region.
   * @param regionName the region to destroy.
   */
  private void destroyRegion(String regionName) {
    Region region = getCache().getRegion(regionName);
    assertNotNull(region);
    
    region.destroyRegion();
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
  
  public static final class StatCompressor implements Compressor, Serializable {
    private static final long serialVersionUID = 8116784819434199537L;

    private final byte[] POST_COMPRESS_BYTES = "12345678901234567890123456789012345678901234567890".getBytes();

    static byte[] savedCompressInput = null;

    public byte[] compress(byte[] input) {
      if (savedCompressInput == null) {
        savedCompressInput = input;
      }
      return POST_COMPRESS_BYTES;
    }

    public byte[] decompress(byte[] input) {
      return savedCompressInput;
    }
  }

  /**
   * Creates a region and assigns a compressor.
   * @param name a region name.
   * @param compressor a compressor.
   */
  private Region createRegion(String name,Compressor compressor) {
    return getCache().createRegionFactory().setDataPolicy(DataPolicy.REPLICATE).setCloningEnabled(true).setCompressor(compressor).create(name);
  }

  /**
   * Enables clock stats on a VM.
   * @param vm a virtual machine
   * @param clockStatsEnabled enables clock stats if true,  disables if false
   * @return previous clock stats value 
   */
  private boolean enableClockStatsOnVm(final VM vm,final boolean clockStatsEnabled) {
    return (Boolean) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        return enableClockStats(clockStatsEnabled);
      }      
    });
  }

  /**
   * Enables clock stats.
   * @param clockStatsEnabled enables clock stats if true,  disables if false
   * @return previous clock stats value
   */
  private boolean enableClockStats(boolean clockStatsEnabled) {
    boolean oldValue = CachePerfStats.enableClockStats;
    
    CachePerfStats.enableClockStats = clockStatsEnabled;
    
    return oldValue;
  }
  
  /**
   * Used to record compression statistics.
   * @author rholmes
   */
  private static final class CompressionStats {
    long vmTotalCompressionTime = 0;     
    long region1TotalCompressionTime = 0;  
    long region2TotalCompressionTime = 0; 

    long vmTotalDecompressionTime = 0; 
    long region1TotalDecompressionTime = 0; 
    long region2TotalDecompressionTime = 0;

    long vmTotalCompressions = 0;
    long region1TotalCompressions = 0;
    long region2TotalCompressions = 0;

    long vmTotalDecompressions = 0;
    long region1TotalDecompressions = 0;
    long region2TotalDecompressions = 0;

    long vmTotalPreCompressedBytes = 0;
    long region1TotalPreCompressedBytes = 0;
    long region2TotalPreCompressedBytes = 0;

    long vmTotalPostCompressedBytes = 0;
    long region1TotalPostCompressedBytes = 0;
    long region2TotalPostCompressedBytes = 0;
  }
}
