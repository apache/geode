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
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.SystemTimer;
import com.gemstone.gemfire.internal.SystemTimer.SystemTimerTask;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.DateFormatter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import org.apache.logging.log4j.Logger;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DSClock tracks the system time.  The most useful method is 
 * cacheTimeMillis().  The rest are for clock adjustments.
 * 
 * Clock adjustments can be turned off with gemfire.disable-distributed-clock
 * 
 */

public class DSClock implements CacheTime {

  private static final Logger logger = LogService.getLogger();
  
  private final int MAX_TIME_OFFSET_DIFF =  100; /* in milliseconds */
  private final long MAX_CACHE_TIME_MILLIS = 0x00FFFFFFFFFFFFFFL;
  /**
   * Time shift received from server must be at least this far off
   * in order for the cacheTimeMillis clock to be changed.  Servers
   * are much more aggressive about it.
   */
  private final static long MINIMUM_TIME_DIFF = 5000; 

  /**
   * cacheTimeMillis offset from System.currentTimeMillis
   */
  private volatile long cacheTimeDelta;
  /**
   * a task to slow down the clock if cacheTimeDelta decreases in value
   */
  private SystemTimerTask cacheTimeTask = null; // task to slow down the clock
  /**
   * State variable used by the cacheTimeTask
   */
  private final AtomicLong suspendedTime = new AtomicLong(0L);
  
  /**
   * Is this a client "distributed system"
   */
  private final boolean isLoner;
  
  /** GemFire internal test hook for unit testing */
  private DSClockTestHook testHook;
  
  
  protected DSClock(boolean lonerDS) {
    this.isLoner = lonerDS;
  }
  
  @Override
  public long cacheTimeMillis() {
    long result;
    final long offset = getCacheTimeOffset();
    final long st = getStopTime();
    if (st != 0) {
      result = st + offset;
      if (result < 0 || result > MAX_CACHE_TIME_MILLIS) {
        throw new IllegalStateException("Expected cacheTimeMillis " + result + " to be >= 0 and <= " + MAX_CACHE_TIME_MILLIS + " stopTime=" + st + " offset=" + offset);
      }
    } else {
      long ct = System.currentTimeMillis();
      result =  ct + offset;
      if (result < 0 || result > MAX_CACHE_TIME_MILLIS) {
        throw new IllegalStateException("Expected cacheTimeMillis " + result + " to be >= 0 and <= " + MAX_CACHE_TIME_MILLIS + " curTime=" + ct + " offset=" + offset);
      }
    }
    return result;
  }

  /**
   * @return Offset for system time, calculated by distributed system coordinator.
   * @since GemFire 8.0
   */
  public long getCacheTimeOffset() {
    return this.cacheTimeDelta;
  }

  /**
   * Sets the deviation of this process's local time from the rest of the GemFire
   * distributed system.
   *
   * @since GemFire 8.0
   */
  public void setCacheTimeOffset(DistributedMember coord, long offset, boolean isJoin) {
    if (Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "disable-distributed-clock")) {
      return;
    }
    if (isLoner) {
      setLonerCacheTimeOffset(offset);
    } else {
      setServerCacheTimeOffset(coord, offset, isJoin);
    }
  }

  // set the time offset in a client cache
  private void setLonerCacheTimeOffset(long offset) {
    if (offset > (this.cacheTimeDelta + MINIMUM_TIME_DIFF)) {
      long theTime = System.currentTimeMillis();
      this.cacheTimeDelta = offset;

      String cacheTime = DateFormatter.formatDate(new Date(theTime + offset));
      logger.info(LocalizedMessage.create(LocalizedStrings.LonerDistributionManager_Cache_Time,
          new Object[] { cacheTime, this.cacheTimeDelta }));
      
    } else if (offset < (this.cacheTimeDelta - MINIMUM_TIME_DIFF)) {
      // We don't issue a warning for client caches
//      if ((this.cacheTimeDelta - offset) >= MAX_TIME_OFFSET_DIFF /* Max offset difference allowed */) {
//        this.logger.warning(LocalizedStrings.DistributionManager_Cache_Time_Offset_Skew_Warning, (this.cacheTimeDelta - offset));
//      }
      this.cacheTimeDelta = offset;
      // We need to suspend the cacheTimeMillis for (cacheTimeDelta - offset) ms.
      cancelAndScheduleNewCacheTimerTask(offset);
    }
  }

  private void setServerCacheTimeOffset(DistributedMember coord, long offset, boolean isJoin) {

    if (isJoin || offset > this.cacheTimeDelta) {
      long theTime = System.currentTimeMillis();
      this.cacheTimeDelta = offset;
      if (this.cacheTimeDelta <= -300000 || 300000 <= this.cacheTimeDelta) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.DistributionManager_Time_Skew_Warning, coord));
      }
      String cacheTime = DateFormatter.formatDate(new Date(theTime + offset));
      if (Math.abs(this.cacheTimeDelta) > 1000) {
        Object src = coord;
        if (src == null) {
          src = "local clock adjustment";
        }
        logger.info(LocalizedMessage.create(LocalizedStrings.DistributionManager_Cache_Time,
            new Object[]{ src, cacheTime, this.cacheTimeDelta }));
      }
    } else if (!isJoin && offset < this.cacheTimeDelta) {
      // We need to suspend the cacheTimeMillis for (cacheTimeDelta - offset) ms.
      if ((this.cacheTimeDelta - offset) >= MAX_TIME_OFFSET_DIFF /* Max offset difference allowed */) {
        logger.warn(LocalizedMessage.create(LocalizedStrings.DistributionManager_Cache_Time_Offset_Skew_Warning, (this.cacheTimeDelta - offset)));
      }

      cancelAndScheduleNewCacheTimerTask(offset);
    }
  }

  /**
   * This method is called by a timer task which takes
   * control of cache time and increments the cache time
   * at each call of this method.
   * 
   * The timer task must be called each millisecond. We need
   * to revisit the method implementation if that condition is
   * changed.
   * @param stw True if Stop the world for this cache for a while.
   */
  private void suspendCacheTimeMillis(boolean stw) {
    // Increment stop time at each call of this method.
    if (stw) {
      long oldSt;
      long newSt;
      do {
        oldSt = this.suspendedTime.get();
        if (oldSt == 0) {
          newSt = System.currentTimeMillis();
        } else {
          newSt = oldSt + 1;
        }
      } while (!this.suspendedTime.compareAndSet(oldSt, newSt));
    } else {
      this.suspendedTime.set(0);
    }
  }

  /**
   * Cancel the previous slow down task (If it exists) and schedule a new one.
   * @param offset
   */
  private void cancelAndScheduleNewCacheTimerTask(long offset) {

    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();

    if (cache != null && !cache.isClosed()) {
      if (this.cacheTimeTask != null) {
        this.cacheTimeTask.cancel();
      }
      cacheTimeTask = new CacheTimeTask(offset);
      SystemTimer timer = cache.getCCPTimer();
      timer.scheduleAtFixedRate(cacheTimeTask, 1/* Start after 1ms */ , 2 /* Run task every 2ms */);
      if (logger.isDebugEnabled()) {
        logger.debug("Started a timer task to suspend cache time for new lower offset of {}ms and current offset is: {}", offset, cacheTimeDelta);
      }
    }
  }

  public long getStopTime() {
    return this.suspendedTime.get();
  }

  /**
   * This timer task makes the cache dependent on this DM, to wait
   * (OR in other words stop it's cacheTimeMillis() to return constant value
   * until System.currentTimeMillis() + newOffset reaches/crosses over that
   * constant time) for difference between old time offset and new one if
   * new one is < old one. Because then we need to slow down the cache time
   * aggressively.
   *
   *
   */
  private class CacheTimeTask extends SystemTimerTask {

    private long lowerCacheTimeOffset = 0L;

    
    public CacheTimeTask(long cacheTimeOffset) {
      super();
      this.lowerCacheTimeOffset = cacheTimeOffset;
    }

    @Override
    public void run2() {
      boolean isCancelled =  false;

      suspendCacheTimeMillis(true);

      long currTime = System.currentTimeMillis();
      long cacheTime = cacheTimeMillis();

      if (testHook != null) {
        testHook.suspendAtBreakPoint(1);
        testHook.addInformation("CacheTime", cacheTime);
        testHook.addInformation("AwaitedTime", currTime + lowerCacheTimeOffset);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("CacheTime: {}ms and currTime with offset: {}", cacheTime, (currTime + this.lowerCacheTimeOffset) + "ms");
      }

      // Resume cache time as system time once cache time has slowed down enough.
      long systemTime = currTime + this.lowerCacheTimeOffset;
        
      if (cacheTime <= systemTime) {
        setCacheTimeOffset(null, this.lowerCacheTimeOffset, true);
        suspendCacheTimeMillis(false);
        this.cancel();
        isCancelled = true;
        if (testHook != null) {
          testHook.suspendAtBreakPoint(2);
          testHook.addInformation("FinalCacheTime", cacheTimeMillis());
        }
      }

      if (testHook != null && isCancelled) {
        testHook.suspendAtBreakPoint(3);
        testHook.addInformation("TimerTaskCancelled", true);
      }
    }

    @Override
    public boolean cancel() {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache != null && !cache.isClosed()) {
        suspendCacheTimeMillis(false);
      }
      return super.cancel();
    }
  }

  public static interface DSClockTestHook {
    public void suspendAtBreakPoint(int breakPoint);
    public void addInformation(Object key, Object value);
    public Object getInformation(Object key);
  }

  public DSClockTestHook getTestHook() {
    return this.testHook;
  }

  public void setTestHook(DSClockTestHook th) {
    this.testHook = th;
  }

}
