/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.client.internal;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.client.internal.PoolImpl.PoolTask;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalDataSerializer.SerializerAttributesHolder;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.logging.LogService;

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
    if (logger.isDebugEnabled()) {
      logger.debug("DataSerializerRecoveryTask - EndpointCrashed. Now have {} endpoints", count);
    }
  }

  @Override
  public void endpointNoLongerInUse(Endpoint endpoint) {
    int count = endpointCount.decrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("DataSerializerRecoveryTask - EndpointNoLongerInUse. Now have {} endpoints",
          count);
    }
  }

  @Override
  public void endpointNowInUse(Endpoint endpoint) {
    int count = endpointCount.incrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("DataSerializerRecoveryTask - EndpointNowInUse. Now have {} endpoints", count);
    }
    if (count == 1) {
      synchronized (recoveryScheduledLock) {
        if (!recoveryScheduled) {
          try {
            recoveryScheduled = true;
            background.execute(new RecoveryTask());
            logger.debug("DataSerializerRecoveryTask - Scheduled Recovery Task");
          } catch (RejectedExecutionException e) {
            // ignore, the timer has been cancelled, which means we're shutting down.
          }
        }
      }
    }
  }

  protected class RecoveryTask extends PoolTask {

    @Override
    public void run2() {
      if (pool.getCancelCriterion().isCancelInProgress()) {
        return;
      }

      synchronized (recoveryScheduledLock) {
        recoveryScheduled = false;
      }

      logger.debug("DataSerializerRecoveryTask - Attempting to recover dataSerializers");
      SerializerAttributesHolder[] holders = InternalDataSerializer.getSerializersForDistribution();
      if (holders.length == 0) {
        return;
      }

      EventID eventId = InternalDataSerializer.generateEventId();
      // Fix for bug:40930
      if (eventId == null) {
        try {
          background.schedule(new RecoveryTask(), pingInterval, TimeUnit.MILLISECONDS);
          recoveryScheduled = true;
        } catch (RejectedExecutionException e) {
          if (!pool.getCancelCriterion().isCancelInProgress()) {
            throw e;
          }
        }
      } else {
        try {
          RegisterDataSerializersOp.execute(pool, holders, eventId);
        } catch (CancelException e) {
          return;
        } catch (RejectedExecutionException e) {
          // This is probably because we've started to shut down.
          if (!pool.getCancelCriterion().isCancelInProgress()) {
            throw e; // weird
          }
        } catch (Exception e) {
          if (pool.getCancelCriterion().isCancelInProgress()) {
            return;
          }

          // If ClassNotFoundException occurred on server, don't retry
          Throwable cause = e.getCause();
          boolean cnfException = false;
          if (cause instanceof ClassNotFoundException) {
            logger.warn("DataSerializerRecoveryTask - Error ClassNotFoundException: {}",
                cause.getMessage());
            cnfException = true;
          }

          if (!recoveryScheduled && !cnfException) {
            logger.warn("DataSerializerRecoveryTask - Error recovering dataSerializers: ",
                e);
            try {
              background.schedule(new RecoveryTask(), pingInterval, TimeUnit.MILLISECONDS);
              recoveryScheduled = true;
            } catch (RejectedExecutionException ex) { // GEODE-1613 - suspect string while shutting
                                                      // down
              if (!background.isTerminated() && !pool.getCancelCriterion().isCancelInProgress()) {
                throw ex;
              }
            }

          }
        } finally {
          pool.releaseThreadLocalConnection();
        }
      }
    }
  }
}
