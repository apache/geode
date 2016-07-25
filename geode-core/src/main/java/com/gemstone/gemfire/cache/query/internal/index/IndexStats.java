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

package com.gemstone.gemfire.cache.query.internal.index;

import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.cache.CachePerfStats;

/**
 * IndexStats tracks statistics about query index use.
 */
public class IndexStats {
  
  //////////////////  Static fields ///////////////////////////
  
  private static final StatisticsType type;

  private static final int numKeysId;
  private static final int numValuesId;
  private static final int numUpdatesId;
  private static final int numUsesId;
  private static final int updateTimeId;
  private static final int useTimeId;
  private static final int updatesInProgressId;
  private static final int usesInProgressId;
  private static final int readLockCountId;
  private static final int numMapIndexKeysId;
  private static final int numBucketIndexesId;

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;

  ////////////////////////  Static methods  ////////////////////////
  
  static {
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    final String numKeysDesc = "Number of keys in this index";
    final String numValuesDesc = "Number of values in this index";
    final String numUpdatesDesc = "Number of updates that have completed on this index";
    final String numUsesDesc = "Number of times this index has been used while executing a query";
    final String updateTimeDesc = "Total time spent updating this index";    

    type = f.createType(
      "IndexStats", 
      "Statistics about a query index",
      new StatisticDescriptor[] {
        f.createLongGauge("numKeys", numKeysDesc, "keys"),
        f.createLongGauge("numValues", numValuesDesc, "values"),
        f.createLongCounter("numUpdates", numUpdatesDesc, "operations"),
        f.createLongCounter("numUses", numUsesDesc, "operations"),
        f.createLongCounter("updateTime", updateTimeDesc, "nanoseconds"),
        f.createLongCounter("useTime", "Total time spent using this index", "nanoseconds"),
        f.createIntGauge("updatesInProgress", "Current number of updates in progress.", "updates"),
        f.createIntGauge("usesInProgress", "Current number of uses in progress.", "uses"),
        f.createIntGauge("readLockCount", "Current number of read locks taken.", "uses"),
        f.createLongGauge("numMapIndexKeys", "Number of keys in this Map index", "keys"),
        f.createIntGauge("numBucketIndexes", "Number of bucket indexes in the partitioned region", "indexes"),
      }
    );

    // Initialize id fields
    numKeysId = type.nameToId("numKeys");
    numValuesId = type.nameToId("numValues");
    numUpdatesId = type.nameToId("numUpdates");
    numUsesId = type.nameToId("numUses");
    updateTimeId = type.nameToId("updateTime");    
    updatesInProgressId = type.nameToId("updatesInProgress");    
    usesInProgressId = type.nameToId("usesInProgress");    
    useTimeId = type.nameToId("useTime");
    readLockCountId = type.nameToId("readLockCount");
    numMapIndexKeysId = type.nameToId("numMapIndexKeys");
    numBucketIndexesId = type.nameToId("numBucketIndexes");
  }
  
  ////////////////////////  Constructors  ////////////////////////

  /**
   * Creates a new <code>CachePerfStats</code> and registers itself
   * with the given statistics factory.
   */
  public IndexStats(StatisticsFactory factory, String indexName) {
    stats = factory.createAtomicStatistics(type, indexName);
  }

  //////////////////////  Accessing Stats  //////////////////////

   public long getNumberOfKeys() {
     return stats.getLong(numKeysId);
   }
   
   public long getNumberOfValues() {
     return stats.getLong(numValuesId); 
   }
   
   public long getNumUpdates() {
     return stats.getLong(numUpdatesId);
   }

   public long getTotalUses() {
     return stats.getLong(numUsesId); 
   }  
  
   public long getTotalUpdateTime() {
     return CachePerfStats.enableClockStats? stats.getLong(updateTimeId) : 0;
   }
   
   public long getUseTime() {
     return CachePerfStats.enableClockStats? stats.getLong(useTimeId) : 0;
   }
   
   public int getReadLockCount() {
     return stats.getInt(readLockCountId);
   }
   
   public long getNumberOfMapIndexKeys() {
     return stats.getLong(numMapIndexKeysId);
   }

   public int getNumberOfBucketIndexes() {
     return stats.getInt(numBucketIndexesId);
   }

  //////////////////////  Updating Stats  //////////////////////
  
   public void incNumUpdates() {
     this.stats.incLong(numUpdatesId, 1);
   }
   public void incNumUpdates(int delta) {
     this.stats.incLong(numUpdatesId, delta);
   }
   public void incNumValues(int delta) {
     this.stats.incLong(numValuesId, delta);
   }
  
   public void updateNumKeys(long numKeys) {
     this.stats.setLong(numKeysId, numKeys);
   }
   
   public void incNumKeys(long numKeys) {
     this.stats.incLong(numKeysId, numKeys);
   }
   
   public void incUpdateTime(long delta) {
     if (CachePerfStats.enableClockStats) {
       this.stats.incLong(updateTimeId, delta);
     }
   }
   
   public void incNumUses() {
     this.stats.incLong(numUsesId, 1);
   }
   public void incUpdatesInProgress(int delta) {
     this.stats.incInt(updatesInProgressId, delta);
   }
   public void incUsesInProgress(int delta) {
     this.stats.incInt(usesInProgressId, delta);
   }
   public void incUseTime(long delta) {
     if (CachePerfStats.enableClockStats) {
       this.stats.incLong(useTimeId, delta);
     }
   }
   public void incReadLockCount(int delta) {
     this.stats.incInt(readLockCountId, delta);
   }
   
   public void incNumMapIndexKeys(long delta) {
     this.stats.incLong(numMapIndexKeysId, delta);
   }
   
   public void incNumBucketIndexes(int delta) {
     this.stats.incInt(numBucketIndexesId, delta);
   }   
  ////// Special Instance Methods /////

  /**
   * Closes these stats so that they can not longer be used.  The
   * stats are closed when the {@linkplain
   * com.gemstone.gemfire.internal.cache.GemFireCacheImpl#close cache} 
   * is closed.
   *
   * @since GemFire 3.5
   */
  void close() {
    this.stats.close();
  }

  /**
   * Returns whether or not these stats have been closed
   *
   * @since GemFire 3.5
   */
  public boolean isClosed() {
    return this.stats.isClosed();
  }


  /**
   * Returns the Statistics instance that stores the cache perf stats.
   * @since GemFire 3.5
   */
  public Statistics getStats() {
    return this.stats;
  }
}

