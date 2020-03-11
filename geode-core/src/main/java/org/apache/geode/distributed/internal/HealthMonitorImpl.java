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

package org.apache.geode.distributed.internal;

import org.apache.logging.log4j.Logger;

import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.admin.internal.GemFireHealthEvaluator;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.HealthListenerMessage;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Implements a thread that monitors the health of the vm it lives in.
 *
 * @since GemFire 3.5
 */
public class HealthMonitorImpl implements HealthMonitor, Runnable {
  private static final Logger logger = LogService.getLogger();

  private final InternalDistributedMember owner;
  private final int id;
  private final ClusterDistributionManager dm;
  private final GemFireHealthEvaluator eval;

  /**
   * The current health status
   *
   * @see GemFireHealth#OKAY_HEALTH
   */
  private GemFireHealth.Health currentStatus;
  private final Thread t;
  private volatile boolean stopRequested = false;

  @MakeNotStatic
  private static int idCtr = 0;

  /* Constructors */
  /*
   * Creates a health monitor given its owner, configuration, and its dm
   */
  public HealthMonitorImpl(InternalDistributedMember owner, GemFireHealthConfig config,
      ClusterDistributionManager dm) {
    this.owner = owner;
    this.id = getNewId();
    this.dm = dm;
    this.eval = new GemFireHealthEvaluator(config, dm);
    this.currentStatus = GemFireHealth.GOOD_HEALTH;
    String threadName = String.format("Health Monitor owned by %s", owner);
    this.t = new LoggingThread(threadName, this);
  }

  /************** HealthMonitor interface implementation ******************/
  @Override
  public int getId() {
    return this.id;
  }

  @Override
  public void resetStatus() {
    this.currentStatus = GemFireHealth.GOOD_HEALTH;
    this.eval.reset();
  }

  @Override
  public String[] getDiagnosis(GemFireHealth.Health healthCode) {
    return this.eval.getDiagnosis(healthCode);
  }

  @Override
  public void stop() {
    if (this.t.isAlive()) {
      this.stopRequested = true;
      this.t.interrupt();
    }
  }

  /* HealthMonitorImpl public methods */
  /**
   * Starts the monitor so that it will periodically do health checks.
   */
  public void start() {
    if (this.stopRequested) {
      throw new RuntimeException(
          "A health monitor can not be started once it has been stopped");
    }
    if (this.t.isAlive()) {
      // it is already running
      return;
    }
    this.t.start();
  }

  /* Runnable interface implementation */

  @Override
  public void run() {
    final int sleepTime = this.eval.getEvaluationInterval() * 1000;
    if (logger.isDebugEnabled()) {
      logger.debug("Starting health monitor.  Health will be evaluated every {} seconds.",
          (sleepTime / 1000));
    }
    try {
      while (!this.stopRequested) {
        // SystemFailure.checkFailure(); dm's stopper will do this
        this.dm.getCancelCriterion().checkCancelInProgress(null);
        Thread.sleep(sleepTime);
        if (!this.stopRequested) {
          GemFireHealth.Health newStatus = this.eval.evaluate();
          if (newStatus != this.currentStatus) {
            this.currentStatus = newStatus;
            HealthListenerMessage msg = HealthListenerMessage.create(getId(), newStatus);
            msg.setRecipient(this.owner);
            this.dm.putOutgoing(msg);
          }
        }
      }

    } catch (InterruptedException ex) {
      // No need to reset interrupt bit, we're exiting.
      if (!this.stopRequested) {
        logger.warn("Unexpected stop of health monitor", ex);
      }
    } finally {
      this.eval.close();
      this.stopRequested = true;
      if (logger.isDebugEnabled()) {
        logger.debug("Stopping health monitor");
      }
    }
  }

  /********** Internal implementation **********/

  private static synchronized int getNewId() {
    idCtr += 1;
    return idCtr;
  }

}
