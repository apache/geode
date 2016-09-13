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
   
package org.apache.geode.distributed.internal;

import org.apache.logging.log4j.Logger;

import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.admin.internal.GemFireHealthEvaluator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.HealthListenerMessage;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThreadGroup;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

/**
 * Implements a thread that monitors the health of the vm it lives in.
 * @since GemFire 3.5
 */
public class HealthMonitorImpl implements HealthMonitor, Runnable {
  private static final Logger logger = LogService.getLogger();
  
  private final InternalDistributedMember owner;
  private final int id;
  private final DistributionManager dm;
  private final GemFireHealthEvaluator eval;

  /** The current health status
   *
   * @see GemFireHealth#OKAY_HEALTH */ 
  private GemFireHealth.Health currentStatus;
  private final Thread t;
  private volatile boolean stopRequested = false;

  private static int idCtr = 0;
  
  /********** Constructors *********/
  /**
   * Creates a health monitor given its owner, configuration, and its dm
   */
  public HealthMonitorImpl(InternalDistributedMember owner,
                           GemFireHealthConfig config,
                           DistributionManager dm) {
    this.owner = owner;
    this.id = getNewId();
    this.dm = dm;
    this.eval = new GemFireHealthEvaluator(config, dm);
    this.currentStatus = GemFireHealth.GOOD_HEALTH;
    ThreadGroup tg = LoggingThreadGroup.createThreadGroup("HealthMonitor Threads", logger);
    this.t = new Thread(tg, this, LocalizedStrings.HealthMonitorImpl_HEALTH_MONITOR_OWNED_BY_0.toLocalizedString(owner));
    this.t.setDaemon(true);
  }
  
  /************** HealthMonitor interface implementation ******************/
  public int getId() {
    return this.id;
  }
  public void resetStatus() {
    this.currentStatus = GemFireHealth.GOOD_HEALTH;
    this.eval.reset();
  }
  public String[] getDiagnosis(GemFireHealth.Health healthCode) {
    return this.eval.getDiagnosis(healthCode);
  }
  public void stop() {
    if (this.t.isAlive()) {
      this.stopRequested = true;
      this.t.interrupt();
    }
  }

  /********** HealthMonitorImpl public methods **********/
  /**
   * Starts the monitor so that it will periodically do health checks. 
   */
  public void start() {
    if (this.stopRequested) {
      throw new RuntimeException(LocalizedStrings.HealthMonitorImpl_A_HEALTH_MONITOR_CAN_NOT_BE_STARTED_ONCE_IT_HAS_BEEN_STOPPED.toLocalizedString());
    }
    if (this.t.isAlive()) {
      // it is already running
      return;
    }
    this.t.start();
  }

  /********** Runnable interface implementation **********/

  public void run() {
    final int sleepTime = this.eval.getEvaluationInterval() * 1000;
    if (logger.isDebugEnabled()) {
      logger.debug("Starting health monitor.  Health will be evaluated every {} seconds.", (sleepTime/1000));
    }
    try {
      while (!this.stopRequested) {
//        SystemFailure.checkFailure(); dm's stopper will do this
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
        logger.warn(LocalizedMessage.create(LocalizedStrings.HealthMonitorImpl_UNEXPECTED_STOP_OF_HEALTH_MONITOR), ex);
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
