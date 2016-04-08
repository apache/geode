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
package com.gemstone.gemfire.cache.client.internal;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.client.internal.PoolImpl.PoolTask;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.InternalDataSerializer.SerializerAttributesHolder;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

public class DataSerializerRecoveryListener extends EndpointManager.EndpointListenerAdapter {
  private static final Logger logger = LogService.getLogger();
  
  private final AtomicInteger endpointCount = new AtomicInteger();
  protected final InternalPool pool;
  protected final ScheduledExecutorService background;
  protected final long pingInterval;
  protected final Object recoveryScheduledLock = new Object();
  protected boolean recoveryScheduled;
  
  public DataSerializerRecoveryListener(ScheduledExecutorService background, InternalPool pool) {
    this.pool = pool;
    this.pingInterval = pool.getPingInterval();
    this.background = background;
  }
  
  @Override
  public void endpointCrashed(Endpoint endpoint) {
    int count = endpointCount.decrementAndGet();
    if(logger.isDebugEnabled()) {
      logger.debug("DataSerializerRecoveryTask - EndpointCrashed. Now have {} endpoints", count);
    }
  }

  @Override
  public void endpointNoLongerInUse(Endpoint endpoint) {
    int count = endpointCount.decrementAndGet();
    if(logger.isDebugEnabled()) {
      logger.debug("DataSerializerRecoveryTask - EndpointNoLongerInUse. Now have {} endpoints", count);
    }
  }

  @Override
  public void endpointNowInUse(Endpoint endpoint) {
    int count  = endpointCount.incrementAndGet();
    if(logger.isDebugEnabled()) {
      logger.debug("DataSerializerRecoveryTask - EndpointNowInUse. Now have {} endpoints", count);
    }
    if(count == 1) {
      synchronized(recoveryScheduledLock) {
        if(!recoveryScheduled) {
          try {
            recoveryScheduled = true;
            background.execute(new RecoveryTask());
            logger.debug("DataSerializerRecoveryTask - Scheduled Recovery Task");
          } catch(RejectedExecutionException e) {
            //ignore, the timer has been cancelled, which means we're shutting down.
          }
        }
      }
    }
  }
  
  protected class RecoveryTask extends PoolTask {

    @Override
    public void run2() {
      if(pool.getCancelCriterion().cancelInProgress() != null) {
        return;
      }
      synchronized(recoveryScheduledLock) {
        recoveryScheduled = false;
      }
      logger.debug("DataSerializerRecoveryTask - Attempting to recover dataSerializers");
      SerializerAttributesHolder[] holders= InternalDataSerializer.getSerializersForDistribution();
      if(holders.length == 0) {
        return;
      }
      EventID eventId = InternalDataSerializer.generateEventId();
      //Fix for bug:40930
      if (eventId == null) {
        try {
          background.schedule(new RecoveryTask(), pingInterval,
              TimeUnit.MILLISECONDS);
          recoveryScheduled = true;
        } catch (RejectedExecutionException e) {
          pool.getCancelCriterion().checkCancelInProgress(e);
          throw e;
        }
      }
      else {
        try {
          RegisterDataSerializersOp.execute(pool, holders, eventId);
        } 
        catch (CancelException e) {
          throw e;
        }
        catch (RejectedExecutionException e) {
          // This is probably because we've started to shut down.
          pool.getCancelCriterion().checkCancelInProgress(e);
          throw e; // weird
        }
        catch(Exception e) {
          pool.getCancelCriterion().checkCancelInProgress(e);
          
          // If ClassNotFoundException occurred on server, don't retry
          Throwable cause = e.getCause();
          boolean cnfException = false;
          if (cause instanceof ClassNotFoundException) {
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.DataSerializerRecoveryListener_ERROR_CLASSNOTFOUNDEXCEPTION,
                cause.getMessage()));
            cnfException = true;
          }
          
          if(!recoveryScheduled && !cnfException) {
            logger.warn(LocalizedMessage.create(
              LocalizedStrings.DataSerializerRecoveryListener_ERROR_RECOVERING_DATASERIALIZERS),
              e);
            background.schedule(new RecoveryTask(), pingInterval, TimeUnit.MILLISECONDS);
            recoveryScheduled = true;
          }
        } finally {
          pool.releaseThreadLocalConnection();
        }
      }
    }
  }
}
