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
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Collection;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.cache.query.internal.index.AbstractIndex.RegionEntryToValuesMap;
import com.gemstone.gemfire.cache.query.internal.index.IndexStore.IndexStoreEntry;
import com.gemstone.gemfire.cache.query.internal.index.MemoryIndexStore.MemoryIndexStoreEntry;
import com.gemstone.gemfire.cache.query.partitioned.PRQueryDUnitHelper;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.dunit.ThreadUtils;

/**
 * This test is similar to {@link ConcurrentIndexUpdateWithoutWLDUnitTest} except
 * that it sets system property gemfire.index.INPLACE_OBJECT_MODIFICATION to false
 * so no reverse map is used for {@link CompactRangeIndex}. 
 * 
 * During validation all region operations are paused for a while. Validation
 * happens multiple time during one test run on a fixed time interval.
 * 
 * @author shobhit
 * 
 */
public class ConcurrentIndexUpdateWithInplaceObjectModFalseDUnitTest extends
    DistributedTestCase {
  
  PRQueryDUnitHelper helper = new PRQueryDUnitHelper("ConcurrentIndexUpdateWithoutWLDUnitTest");
  private static String regionName = "Portfolios";
  private int redundancy = 1;
  
  // CompactRangeIndex
  private String indexName = "idIndex";
  private String indexedExpression = "ID";
  private String fromClause = "/"+regionName;
  private String alias = "p";

  private String rindexName = "secidIndex";
  private String rindexedExpression = "pos.secId";
  private String rfromClause = "/"+regionName+" p, p.positions.values pos";
  private String ralias = "pos";

  int stepSize = 10;
  private int totalDataSize = 50;

  /**
   * @param name
   */
  public ConcurrentIndexUpdateWithInplaceObjectModFalseDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    Invoke.invokeInEveryVM(new CacheSerializableRunnable("Set INPLACE_OBJECT_MODIFICATION false") {
      
      @Override
      public void run2() throws CacheException {
        //System.setProperty("gemfire.index.INPLACE_OBJECT_MODIFICATION", "false");
        IndexManager.INPLACE_OBJECT_MODIFICATION_FOR_TEST = true;
      }
    });
    super.setUp();
  }

  /**
   * Tear down a PartitionedRegionTestCase by cleaning up the existing cache
   * (mainly because we want to destroy any existing PartitionedRegions)
   */
  @Override
  protected final void preTearDown() throws Exception {
    Invoke.invokeInEveryVM(new CacheSerializableRunnable("Set INPLACE_OBJECT_MODIFICATION false") {
      
      @Override
      public void run2() throws CacheException {
        //System.setProperty("gemfire.index.INPLACE_OBJECT_MODIFICATION", "false");
        IndexManager.INPLACE_OBJECT_MODIFICATION_FOR_TEST = false;
      }
    });
    Invoke.invokeInEveryVM(ConcurrentIndexUpdateWithInplaceObjectModFalseDUnitTest.class, "destroyRegions");
    Invoke.invokeInEveryVM(CacheTestCase.class, "closeCache");
  }

  public static synchronized void destroyRegions() {
    Cache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      Region region = cache.getRegion(regionName);
      if (region != null) region.destroyRegion();
    }
  }

  // Tests on Local/Replicated Region
  public void testCompactRangeIndex() {
    // Create a Local Region.
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    vm0.invoke(helper.getCacheSerializableRunnableForReplicatedRegionCreation(regionName, Portfolio.class));
    
    vm0.invoke(helper.getCacheSerializableRunnableForPRIndexCreate(regionName, indexName, indexedExpression, fromClause, alias));
    
    AsyncInvocation[] asyncInvs = new AsyncInvocation[2];
    
    asyncInvs[0] = vm0.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, 0, stepSize));
    
    asyncInvs[1] = vm0.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, 0, stepSize));
    
    for (AsyncInvocation inv : asyncInvs) {
      ThreadUtils.join(inv, 30*000);
    }
    
    for (AsyncInvocation inv : asyncInvs) {
      if (inv.exceptionOccurred()) {
        Assert.fail("Random region operation failed on VM_"+inv.getId(), inv.getException());
      }
    }
    
    vm0.invoke(getCacheSerializableRunnableForIndexValidation(regionName, indexName));
    
  }
  

  private SerializableRunnableIF getCacheSerializableRunnableForIndexValidation(
      final String regionName, final String indexName) {
    return new CacheSerializableRunnable("Index Validate") {
      @Override
      public void run2() throws CacheException {
        Cache cache = helper.getCache();
        Region region = cache.getRegion(regionName);
        
        IndexValidator validator = new IndexValidator();
        
        validator.validate(region);
      }
    };
  }

  public void testRangeIndex() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    vm0.invoke(helper.getCacheSerializableRunnableForReplicatedRegionCreation(regionName, Portfolio.class));
    
    vm0.invoke(helper.getCacheSerializableRunnableForPRIndexCreate(regionName, rindexName, rindexedExpression, rfromClause, ralias));
    
    AsyncInvocation[] asyncInvs = new AsyncInvocation[2];
    
    asyncInvs[0] = vm0.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, 0, totalDataSize));
    
    asyncInvs[1] = vm0.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, 0, totalDataSize));
    
    for (AsyncInvocation inv : asyncInvs) {
      ThreadUtils.join(inv, 30*000);
    }
    for (AsyncInvocation inv : asyncInvs) {
      if (inv.exceptionOccurred()) {
        Assert.fail("Random region operation failed on VM_"+inv.getId(), inv.getException());
      }
    }
    
    vm0.invoke(getCacheSerializableRunnableForIndexValidation(regionName, rindexName));
    
  }

  // Tests on Partition Region
  public void testCompactRangeIndexOnPR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);    

    vm0.invoke(helper.getCacheSerializableRunnableForPRAccessorCreate(regionName, redundancy, Portfolio.class));
    
    vm1.invoke(helper.getCacheSerializableRunnableForPRCreate(regionName, redundancy, Portfolio.class));
    
    vm2.invoke(helper.getCacheSerializableRunnableForPRCreate(regionName, redundancy, Portfolio.class));
    
    vm3.invoke(helper.getCacheSerializableRunnableForPRCreate(regionName, redundancy, Portfolio.class));

    vm0.invoke(helper.getCacheSerializableRunnableForPRIndexCreate(regionName, indexName, indexedExpression, fromClause, alias));
    
    
    AsyncInvocation[] asyncInvs = new AsyncInvocation[12];
    
    asyncInvs[0] = vm0.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, 0, stepSize));
    
    asyncInvs[1] = vm1.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, stepSize, (2 * stepSize)));
    
    asyncInvs[2] = vm2.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, (2 * stepSize), (3 * stepSize)));
    
    asyncInvs[3] = vm3.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, (3 * (stepSize)), totalDataSize )); 
    
    asyncInvs[4] = vm0.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, 0, stepSize));
    
    asyncInvs[5] = vm1.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, stepSize, (2 * stepSize)));
    
    asyncInvs[6] = vm2.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, (2 * stepSize), (3 * stepSize)));
    
    asyncInvs[7] = vm3.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, (3 * (stepSize)), totalDataSize )); 
    
    asyncInvs[8] = vm0.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, 0, stepSize));
    
    asyncInvs[9] = vm1.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, stepSize, (2 * stepSize)));
    
    asyncInvs[10] = vm2.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, (2 * stepSize), (3 * stepSize)));
    
    asyncInvs[11] = vm3.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, (3 * (stepSize)), totalDataSize ));
    
    for (AsyncInvocation inv : asyncInvs) {
      ThreadUtils.join(inv, 60*000);
    }
    
    for (AsyncInvocation inv : asyncInvs) {
      if (inv.exceptionOccurred()) {
        Assert.fail("Random region operation failed on VM_"+inv.getId(), inv.getException());
      }
    }
    vm0.invoke(getCacheSerializableRunnableForIndexValidation(regionName, indexName));
    
    vm1.invoke(getCacheSerializableRunnableForIndexValidation(regionName, indexName));
    
    vm2.invoke(getCacheSerializableRunnableForIndexValidation(regionName, indexName));
    
    vm3.invoke(getCacheSerializableRunnableForIndexValidation(regionName, indexName));
  }
  

  public void testRangeIndexOnPR() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);    

    vm0.invoke(helper.getCacheSerializableRunnableForPRAccessorCreate(regionName, redundancy, Portfolio.class));
    
    vm1.invoke(helper.getCacheSerializableRunnableForPRCreate(regionName, redundancy, Portfolio.class));
    
    vm2.invoke(helper.getCacheSerializableRunnableForPRCreate(regionName, redundancy, Portfolio.class));
    
    vm3.invoke(helper.getCacheSerializableRunnableForPRCreate(regionName, redundancy, Portfolio.class));

    vm0.invoke(helper.getCacheSerializableRunnableForPRIndexCreate(regionName, rindexName, rindexedExpression, rfromClause, ralias));
    
    
    AsyncInvocation[] asyncInvs = new AsyncInvocation[12];
    
    asyncInvs[0] = vm0.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, 0, stepSize));
    
    asyncInvs[1] = vm1.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, stepSize, (2 * stepSize)));
    
    asyncInvs[2] = vm2.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, (2 * stepSize), (3 * stepSize)));
    
    asyncInvs[3] = vm3.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, (3 * (stepSize)), totalDataSize )); 
    
    asyncInvs[4] = vm0.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, 0, stepSize));
    
    asyncInvs[5] = vm1.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, stepSize, (2 * stepSize)));
    
    asyncInvs[6] = vm2.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, (2 * stepSize), (3 * stepSize)));
    
    asyncInvs[7] = vm3.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, (3 * (stepSize)), totalDataSize )); 
    
    asyncInvs[8] = vm0.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, 0, stepSize));
    
    asyncInvs[9] = vm1.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, stepSize, (2 * stepSize)));
    
    asyncInvs[10] = vm2.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, (2 * stepSize), (3 * stepSize)));
    
    asyncInvs[11] = vm3.invokeAsync(helper.getCacheSerializableRunnableForPRRandomOps(regionName, (3 * (stepSize)), totalDataSize ));
    
    for (AsyncInvocation inv : asyncInvs) {
      ThreadUtils.join(inv, 60*000);
    }
    for (AsyncInvocation inv : asyncInvs) {
      if (inv.exceptionOccurred()) {
        Assert.fail("Random region operation failed on VM_"+inv.getId(), inv.getException());
      }
    }
    vm0.invoke(getCacheSerializableRunnableForIndexValidation(regionName, rindexName));
    
    vm1.invoke(getCacheSerializableRunnableForIndexValidation(regionName, rindexName));
    
    vm2.invoke(getCacheSerializableRunnableForIndexValidation(regionName, rindexName));
    
    vm3.invoke(getCacheSerializableRunnableForIndexValidation(regionName, rindexName));
  }

  /**
   * This validator will iterate over RegionEntries and verify their corresponding
   * index key and entry presence in index valuesToEntriesMap.
   * 
   * @author shobhit
   *
   */
  public class IndexValidator {

    public IndexValidator() {
      
    }

    private boolean isValidationInProgress;
    
    /**
     * Validation is done in the end of test on all indexes of a region
     * by verifying last on a region key and verifying state of index based
     * on the last operation.
     *
     * @param region being validated for all of its indexes.
     */
    public void validate(Region region) {
      // Get List of All indexes.
      Collection<Index> indexes = ((LocalRegion)region).getIndexManager().getIndexes();
      
      // validate each index one by one
      for (Index index : indexes) {
        if (region instanceof PartitionedRegion) {
          validateOnPR((PartitionedRegion)region, (PartitionedIndex)index);
        } else {
          validate(region, index);
        }
      }
    }

    private void validate(Region region, Index index) {
      // Get index expression
      String indexExpr = index.getIndexedExpression();
      int expectedIndexSize = 0;
      int expectedNullEntries = 0;
      int expectedUndefinedEntries = 0;
      
      // Lets check if it contains a '.'
      if (indexExpr.indexOf(".") != -1) {
        indexExpr = indexExpr.substring(indexExpr.indexOf(".")+1);
      }

      // do a get<indexExpr>() on each region value and verify if the
      // evaluated index key is part of index and has RE as a reference to it
      Collection<RegionEntry> entries = ((LocalRegion)region).entries.regionEntries();
      for (RegionEntry internalEntry : entries) {
        Object value = internalEntry.getValueInVM((LocalRegion) region);
        
        if (value instanceof CachedDeserializable) {
          value = ((CachedDeserializable) value).getDeserializedValue(region,
              internalEntry);
        }
        if (indexExpr.equals("ID")) {
          // Compact Range Index
          if (index instanceof CompactRangeIndex) {
            // Ignore invalid values.
            if (value != Token.INVALID && value != Token.TOMBSTONE) {
              LogWriterUtils.getLogWriter().info("Portfolio: "+ ((Portfolio)value));
              Integer ID = ((Portfolio) value).getID();

              assertTrue("Did not find index key for REgionEntry [key: "
                  + internalEntry.getKey() + " , value: " + value
                  + " ] in index: " + index.getName(),
                  ((CompactRangeIndex) index).getIndexStorage().get(ID) == null ? false : true);

              // Get Index value for the evaluated index key.
              CloseableIterator<IndexStoreEntry> valuesForKeyIterator = null;
              
              try {
                valuesForKeyIterator = ((CompactRangeIndex) index)
                    .getIndexStorage().get(ID);

                // Check if RegionEntry is present in Index for the key
                // evaluated from
                // region value.
                while (valuesForKeyIterator.hasNext()) {
                  assertTrue("Did not find index value for REgionEntry [key: "
                      + internalEntry.getKey() + " , value: " + value
                      + " ] in index: " + index.getName() + " For index key: "
                      + ID,
                      (((MemoryIndexStoreEntry) valuesForKeyIterator.next())
                          .getRegionEntry() == internalEntry));
                }
              } finally {
                if (valuesForKeyIterator != null) {
                  valuesForKeyIterator.close();
                }
              }

              if (ID != IndexManager.NULL) {
                expectedIndexSize++;
              } else {
                expectedNullEntries++;
              }
            } else {
              LogWriterUtils.getLogWriter().info(internalEntry.getKey()+"");
              expectedUndefinedEntries++;
            }
          }

        } else if (indexExpr.equals("secId")) {
          if (index instanceof RangeIndex) {
            // Ignore invalid values.
            if (value != Token.INVALID && value != Token.TOMBSTONE) {
              Collection<Position> positions = ((Portfolio)value).positions.values();
              for (Position pos : positions) {
                if (pos != null) {
                  LogWriterUtils.getLogWriter().info("Portfolio: "+ ((Portfolio)value) + "Position: " + pos);
                  String secId = pos.secId;
                  assertTrue("Did not find index key for REgionEntry [key: "
                      + internalEntry.getKey() + " , value: " + value
                      + " ] in index: " + index.getName(),
                      ((RangeIndex) index).valueToEntriesMap.containsKey(secId));
                  
                  // Get Index value for the evaluated index key.
                  Object valuesForKey = ((RangeIndex) index).valueToEntriesMap
                      .get(secId);

                  // Check if RegionEntry is present in Index for the key evaluated from
                  // region value.
                  if (! (valuesForKey instanceof RegionEntryToValuesMap)) {
                    assertTrue(
                        "Did not find index value for REgionEntry [key: "
                            + internalEntry.getKey() + " , value: " + value
                            + " ] in index: " + index.getName() + " For index key: "
                            + secId, ((RegionEntry)valuesForKey == internalEntry));
                  } else {
                    assertTrue(
                        "Did not find index value for REgionEntry [key: "
                            + internalEntry.getKey() + " , value: " + value
                            + " ] in index: " + index.getName() + " For index key: "
                            + secId, (((RegionEntryToValuesMap)valuesForKey).containsEntry(internalEntry)));
                  }
    
                  if (secId != null) {
                    expectedIndexSize++;
                  } else {
                    expectedNullEntries++;
                  }
                } else {
                  expectedUndefinedEntries++;
                }
              }
            }
          }
        }  
      }
      
      // Validate sizes for index map, null and undefined maps.
      int actualSize = 0;
      if (index instanceof CompactRangeIndex) {
        CloseableIterator<IndexStoreEntry> iter = null;
        try {
          iter = ((CompactRangeIndex) index).getIndexStorage().iterator(null);
          while (iter.hasNext()) {
            Object value = iter.next().getDeserializedValue();
//            getLogWriter().info(
//                "Index Values : " + value);
            actualSize++;
          }
        } finally {
          if (iter != null) {
            iter.close();
          }
        }
      } 
      if (index instanceof RangeIndex) {
        for (Object value : ((RangeIndex) index).valueToEntriesMap.values()) {
          if (value instanceof RegionEntry) {
            actualSize++;
          } else {
//            for (Object obj: ((RegionEntryToValuesMap)value).map.values()) {
//              getLogWriter().info("Index Values : "+ obj.toString());
//            }
            actualSize += ((RegionEntryToValuesMap)value).getNumValues();
          }
        }
      }

      IndexStatistics stats = index.getStatistics();
      if (index instanceof CompactRangeIndex) {
/*        getLogWriter().info(
            " Actual Size of Index is: " + actualSize + " Undefined size is: "
                + ((CompactRangeIndex) index).undefinedMappedEntries.size()
                + " And NULL size is: "
                + ((CompactRangeIndex) index).nullMappedEntries.size());
        for (Object obj : ((CompactRangeIndex) index).undefinedMappedEntries
            .toArray()) {
          getLogWriter().info(((RegionEntry) obj).getKey() + "");
        }
*/        LogWriterUtils.getLogWriter().info(
            " Expected Size of Index is: " + expectedIndexSize
                + " Undefined size is: " + expectedUndefinedEntries
                + " And NULL size is: " + expectedNullEntries);
        assertEquals("No of index keys NOT equals the no shown in statistics for index:" + index.getName(), ((CompactRangeIndex) index).getIndexStorage().size(), stats.getNumberOfKeys());
      } else {
        LogWriterUtils.getLogWriter().info(
            " Actual Size of Index is: " + actualSize + " Undefined size is: "
                + ((RangeIndex) index).undefinedMappedEntries.getNumEntries()
                + " And NULL size is: "
                + ((RangeIndex) index).nullMappedEntries.getNumEntries());
        for (Object obj : ((RangeIndex) index).undefinedMappedEntries.map.keySet()) {
          LogWriterUtils.getLogWriter().info(((RegionEntry) obj).getKey() + "");
        }
        LogWriterUtils.getLogWriter().info(
            " Expected Size of Index is: " + expectedIndexSize
                + " Undefined size is: " + expectedUndefinedEntries
                + " And NULL size is: " + expectedNullEntries);
        assertEquals("No of index keys NOT equals the no shown in statistics for index:" + index.getName(), ((RangeIndex) index).valueToEntriesMap.keySet().size(), stats.getNumberOfKeys());
      }
      assertEquals("No of index entries NOT equal the No of RegionEntries Basec on statistics for index:" + index.getName(), (expectedIndexSize + expectedNullEntries), stats.getNumberOfValues());
      assertEquals("No of index entries NOT equals the No of RegionEntries for index:" + index.getName(), expectedIndexSize, actualSize);
      GemFireCacheImpl.getInstance().getLogger().fine("Finishing the validation for region: "+ region.getFullPath() + " and Index: "+ index.getName());
    }
    
    private void validateOnPR(PartitionedRegion pr, PartitionedIndex ind) {
      // Get index expression
      String indexExpr = ind.getIndexedExpression();
      int expectedIndexSize = 0;
      int expectedNullEntries = 0;
      int expectedUndefinedEntries = 0;
      
      // Lets check if it contains a '.'
      if (indexExpr.indexOf(".") != -1) {
        indexExpr = indexExpr.substring(indexExpr.indexOf(".")+1);
      }
      
      int actualValueSize = 0;
      int actualKeySize = 0;

      for (Object idx: ind.getBucketIndexes()) {
        
        Index index = (Index)idx;
        assertTrue("Bucket stats are different than PR stats for bucket: "+ index.getRegion(), index.getStatistics() == ind.getStatistics());
        Region region = index.getRegion();
        // do a get<indexExpr>() on each region value and verify if the
        // evaluated index key is part of index and has RE as a reference to it
        Collection<RegionEntry> entries = ((LocalRegion)region).entries.regionEntries();
        for (RegionEntry internalEntry : entries) {
          Object value = internalEntry.getValueInVM((LocalRegion) region);
          
          if (value instanceof CachedDeserializable) {
            value = ((CachedDeserializable) value).getDeserializedValue(region,
                internalEntry);
          }
          if (indexExpr.equals("ID")) {
            // Compact Range Index
            if (index instanceof CompactRangeIndex) {
              // Ignore invalid values.
              if (value != Token.INVALID && value != Token.TOMBSTONE) {
                LogWriterUtils.getLogWriter().info("Portfolio: "+ ((Portfolio)value));
                Integer ID = ((Portfolio) value).getID();
  
                assertTrue("Did not find index key for REgionEntry [key: "
                    + internalEntry.getKey() + " , value: " + value
                    + " ] in index: " + index.getName(),
                    ((CompactRangeIndex) index).getIndexStorage().get(ID) == null ? false : true);
                
                // Get Index value for the evaluated index key.
                CloseableIterator<IndexStoreEntry> valuesForKeyIterator = null;
                try {
                  valuesForKeyIterator = ((CompactRangeIndex) index)
                      .getIndexStorage().get(ID);

                  // Check if RegionEntry is present in Index for the key
                  // evaluated from
                  // region value.
                  while (valuesForKeyIterator.hasNext()) {
                    assertTrue(
                        "Did not find index value for REgionEntry [key: "
                            + internalEntry.getKey() + " , value: " + value
                            + " ] in index: " + index.getName()
                            + " For index key: " + ID,
                        (((MemoryIndexStoreEntry) valuesForKeyIterator.next())
                            .getRegionEntry() == internalEntry));
                  }
                } finally {
                  if (valuesForKeyIterator != null) {
                    valuesForKeyIterator.close();
                  }
                }
                if (ID != IndexManager.NULL) {
                  expectedIndexSize++;
                } else {
                  expectedNullEntries++;
                }
              } else {
                expectedUndefinedEntries++;
              }
            }
  
          } else if (indexExpr.equals("secId")) {
            if (index instanceof RangeIndex) {
              // Ignore invalid values.
              if (value != Token.INVALID && value != Token.TOMBSTONE) {
                Collection<Position> positions = ((Portfolio)value).positions.values();
                for (Position pos : positions) {
                  if (pos != null) {
                    LogWriterUtils.getLogWriter().info("Portfolio: "+ ((Portfolio)value) + "Position: " + pos);
                    String secId = pos.secId;
                    assertTrue("Did not find index key for REgionEntry [key: "
                        + internalEntry.getKey() + " , value: " + value
                        + " ] in index: " + index.getName(),
                        ((RangeIndex) index).valueToEntriesMap.containsKey(secId));
                    
                    // Get Index value for the evaluated index key.
                    Object valuesForKey = ((RangeIndex) index).valueToEntriesMap
                        .get(secId);
  
                    // Check if RegionEntry is present in Index for the key evaluated from
                    // region value.
                    if (! (valuesForKey instanceof RegionEntryToValuesMap)) {
                      assertTrue(
                          "Did not find index value for REgionEntry [key: "
                              + internalEntry.getKey() + " , value: " + value
                              + " ] in index: " + index.getName() + " For index key: "
                              + secId, ((RegionEntry)valuesForKey == internalEntry));
                    } else {
                      assertTrue(
                          "Did not find index value for REgionEntry [key: "
                              + internalEntry.getKey() + " , value: " + value
                              + " ] in index: " + index.getName() + " For index key: "
                              + secId, (((RegionEntryToValuesMap)valuesForKey).containsEntry(internalEntry)));
                    }
      
                    if (secId != null) {
                      expectedIndexSize++;
                    } else {
                      expectedNullEntries++;
                    }
                  } else {
                    expectedUndefinedEntries++;
                  }
                }
              }
            }
          }  
        }
        
        // Validate sizes for index map, null and undefined maps.
        if (index instanceof CompactRangeIndex) {
          CloseableIterator<IndexStoreEntry> iter = null;
          try {
            iter = ((CompactRangeIndex) index).getIndexStorage().iterator(null);
            while (iter.hasNext()) {
              Object value = iter.next().getDeserializedValue();
//              getLogWriter().info(
//                  "Index Values : " + value);
              actualValueSize++;
            }
          } finally {
            if (iter != null) {
              iter.close();
            }
          }
        } 
        if (index instanceof RangeIndex) {
          for (Object value : ((RangeIndex) index).valueToEntriesMap.values()) {
            if (value instanceof RegionEntry) {
              actualValueSize++;
            } else {
              actualValueSize += ((RegionEntryToValuesMap)value).getNumValues();
            }
          }
        }
        if (index instanceof CompactRangeIndex) {
          actualKeySize += ((CompactRangeIndex) index).getIndexStorage().size();
        } else {
          actualKeySize += ((RangeIndex) index).valueToEntriesMap.keySet().size();
        }
      }
      assertEquals("No of index entries NOT equals the No of RegionENtries NOT based on stats for index:" + ind.getName(), expectedIndexSize, actualValueSize);
      IndexStatistics stats = ind.getStatistics();
      assertEquals("No of index entries NOT equals the No of RegionENtries based on statistics for index:" + ind.getName(), (expectedIndexSize + expectedNullEntries), stats.getNumberOfValues());
      GemFireCacheImpl.getInstance().getLogger().fine("Finishing the validation for region: "+ pr.getFullPath() + " and Index: "+ ind.getName());
    }
  }
}
