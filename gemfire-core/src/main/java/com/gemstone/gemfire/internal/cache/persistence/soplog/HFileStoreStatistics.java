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
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import static com.gemstone.gemfire.distributed.internal.DistributionStats.getStatTime;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.internal.DummyStatisticsFactory;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;

public class HFileStoreStatistics {
  private final Statistics stats;
  
  private final CacheOperation blockCache;
  
  public HFileStoreStatistics(String typeName, String name) {
    this(new DummyStatisticsFactory(), typeName, name);
  }
  
  public HFileStoreStatistics(StatisticsFactory factory, String typeName, String name) {
    StatisticsTypeFactory tf = StatisticsTypeFactoryImpl.singleton();
    
    StatisticDescriptor bcMisses = tf.createLongCounter("blockCacheMisses", "The total number of block cache misses", "misses");
    StatisticDescriptor bcHits = tf.createLongCounter("blockCacheHits", "The total number of block cache hits", "hits");
    StatisticDescriptor bcCached = tf.createLongGauge("blocksCached", "The current number of cached blocks", "blocks");
    StatisticDescriptor bcBytesCached = tf.createLongGauge("blockBytesCached", "The current number of bytes cached", "bytes");
    StatisticDescriptor bcBytesEvicted = tf.createLongCounter("blockBytesEvicted", "The total number of bytes cached", "bytes");

    
    StatisticsType type = tf.createType(typeName, 
        "Statistics about structured I/O operations for a region", new StatisticDescriptor[] {
        bcMisses, bcHits, bcCached, bcBytesCached, bcBytesEvicted
    });

    blockCache = new CacheOperation(bcMisses.getId(), bcHits.getId(), bcCached.getId(), bcBytesCached.getId(), bcBytesEvicted.getId());


    stats = factory.createAtomicStatistics(type, name);
  }

  public void close() {
    stats.close();
  }
  
  public Statistics getStats() {
    return stats;
  }
  
  public CacheOperation getBlockCache() {
    return blockCache;
  }
  
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("blockCache = {").append(blockCache).append("}\n");
    
    return sb.toString();
  }
  
  public class TimedOperation {
    protected final int countId;
    protected final int inProgressId;
    protected final int timeId;
    private final int errorsId;
    
    public TimedOperation(int count, int inProgress, int time, int errors) {
      this.countId = count;
      this.inProgressId = inProgress;
      this.timeId = time;
      this.errorsId = errors;
    }
    
    public long begin() {
      stats.incLong(inProgressId, 1);
      return getStatTime();
    }
    
    public long end(long start) {
      stats.incLong(inProgressId, -1);
      stats.incLong(countId, 1);
      stats.incLong(timeId, getStatTime() - start);
      return getStatTime();
    }
    
    public void error(long start) {
      end(start);
      stats.incLong(errorsId, 1);
    }
    
    public long getCount() {
      return stats.getLong(countId);
    }
    
    public long getInProgress() {
      return stats.getLong(inProgressId);
    }
    
    public long getTime() {
      return stats.getLong(timeId);
    }
    
    public long getErrors() {
      return stats.getLong(errorsId);
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("count=").append(getCount());
      sb.append(";inProgress=").append(getInProgress());
      sb.append(";errors=").append(getErrors());
      sb.append(";time=").append(getTime());
      
      return sb.toString();
    }
  }
  
  public class CacheOperation {
    private final int missesId;
    private final int hitsId;
    private final int cachedId;
    private final int bytesCachedId;
    private final int bytesEvictedId;
    
    public CacheOperation(int missesId, int hitsId, int cachedId, 
        int bytesCachedId, int bytesEvictedId) {
      this.missesId = missesId;
      this.hitsId = hitsId;
      this.cachedId = cachedId;
      this.bytesCachedId = bytesCachedId;
      this.bytesEvictedId = bytesEvictedId;
    }
    
    public void store(long bytes) {
      stats.incLong(cachedId, 1);
      stats.incLong(bytesCachedId, bytes);
    }
    
    public void evict(long bytes) {
      stats.incLong(cachedId, -1);
      stats.incLong(bytesCachedId, -bytes);
      stats.incLong(bytesEvictedId, bytes);
    }
    
    public void hit() {
      stats.incLong(hitsId, 1);
    }
    
    public void miss() {
      stats.incLong(missesId, 1);
    }
    
    public long getMisses() {
      return stats.getLong(missesId);
    }
    
    public long getHits() {
      return stats.getLong(hitsId);
    }
    
    public long getCached() {
      return stats.getLong(cachedId);
    }
    
    public long getBytesCached() {
      return stats.getLong(bytesCachedId);
    }
    
    public long getBytesEvicted() {
      return stats.getLong(bytesEvictedId);
    }
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("misses=").append(getMisses());
      sb.append(";hits=").append(getHits());
      sb.append(";cached=").append(getCached());
      sb.append(";bytesCached=").append(getBytesCached());
      sb.append(";bytesEvicted=").append(getBytesEvicted());
      
      return sb.toString();
    }
  }
}
