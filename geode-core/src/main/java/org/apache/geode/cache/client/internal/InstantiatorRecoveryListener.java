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
import org.apache.geode.internal.InternalInstantiator;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.logging.LogService;

/**
 * A listener which will try to resend the instantiators to all servers if the entire server
 * distributed system was lost and came back one line. This listener also takes care of sending the
 * initial list of instantiators to the servers <br>
 * <br>
 * TODO - There is a window in which all of the servers could crash and come back up and we would
 * connect to a new server before realizing that all the servers crashed. To fix this, we would need
 * to get some kind of birthdate of the server ds we connect and use that to decide if we need to
 * recover instantiators. As it is, the window is not very large.
 *
 *
 *
 */
public class InstantiatorRecoveryListener extends EndpointManager.EndpointListenerAdapter {
  private static final Logger logger = LogService.getLogger();

  private final AtomicInteger endpointCount = new AtomicInteger();
  protected final InternalPool pool;
  protected final ScheduledExecutorService background;
  protected final long pingInterval;
  protected final Object recoveryScheduledLock = new Object();
  protected boolean recoveryScheduled;

  public InstantiatorRecoveryListener(ScheduledExecutorService background, InternalPool pool) {
    this.pool = pool;
    this.pingInterval = pool.getPingInterval();
    this.background = background;
  }

  @Override
  public void endpointCrashed(Endpoint endpoint) {
    int count = endpointCount.decrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("InstantiatorRecoveryTask - EndpointCrashed. Now have {} endpoints", count);
    }
  }

  @Override
  public void endpointNoLongerInUse(Endpoint endpoint) {
    int count = endpointCount.decrementAndGet();
    if (logger.isDebugEnabled()) {
      logger.debug("InstantiatorRecoveryTask - EndpointNoLongerInUse. Now have {} endpoints",
          count);
    }
  }

  @Override
  public void endpointNowInUse(Endpoint endpoint) {
    int count = endpointCount.incrementAndGet();
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("InstantiatorRecoveryTask - EndpointNowInUse. Now have {} endpoints", count);
    }
    if (count == 1) {
      synchronized (recoveryScheduledLock) {
        if (!recoveryScheduled) {
          try {
            recoveryScheduled = true;
            background.execute(new RecoveryTask());
            if (isDebugEnabled) {
              logger.debug("InstantiatorRecoveryTask - Scheduled Recovery Task");
            }
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
      Object[] objects = InternalInstantiator.getInstantiatorsForSerialization();
      if (objects.length == 0) {
        return;
      }
      EventID eventId = InternalInstantiator.generateEventId();
      // Fix for bug:40930
      if (eventId == null) {
        background.schedule(new RecoveryTask(), pingInterval, TimeUnit.MILLISECONDS);
        recoveryScheduled = true;
      } else {
        try {
          RegisterInstantiatorsOp.execute(pool, objects, eventId);
        } catch (CancelException e) {
          throw e;
        } catch (RejectedExecutionException e) {
          // This is probably because we've started to shut down.
          pool.getCancelCriterion().checkCancelInProgress(e);
          throw e; // weird
        } catch (Exception e) {
          pool.getCancelCriterion().checkCancelInProgress(e);

          // If an exception occurred on the server, don't retry
          Throwable cause = e.getCause();
          if (cause instanceof ClassNotFoundException) {
            logger.warn("InstantiatorRecoveryTask - Error ClassNotFoundException: {}",
                cause.getMessage());
          } else {
            logger.warn("InstantiatorRecoveryTask - Error recovering instantiators:",
                e);
          }
        } finally {
          pool.releaseThreadLocalConnection();
        }
      }
    }
  }
}
