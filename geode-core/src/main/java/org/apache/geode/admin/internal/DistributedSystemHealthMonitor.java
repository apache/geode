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
package org.apache.geode.admin.internal;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.SystemFailure;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.admin.GemFireMemberStatus;
import org.apache.geode.admin.RegionSubRegionSnapshot;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.Config;
import org.apache.geode.internal.admin.AdminBridgeServer;
import org.apache.geode.internal.admin.CacheInfo;
import org.apache.geode.internal.admin.DLockInfo;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.admin.GfManagerAgent;
import org.apache.geode.internal.admin.HealthListener;
import org.apache.geode.internal.admin.Stat;
import org.apache.geode.internal.admin.StatAlertDefinition;
import org.apache.geode.internal.admin.StatListener;
import org.apache.geode.internal.admin.StatResource;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThread;
import org.apache.geode.internal.net.SocketCreator;

/**
 * A thread that monitors the health of the distributed system. It is kind of like a
 * {@link org.apache.geode.distributed.internal.HealthMonitorImpl}. In order to get it to place nice
 * with the rest of the health monitoring APIs, this class pretends that it is a
 * <code>GemFireVM</code>. Kind of hokey, but it beats a bunch of special-case code.
 *
 *
 * @since GemFire 3.5
 */
class DistributedSystemHealthMonitor implements Runnable, GemFireVM {

  private static final Logger logger = LogService.getLogger();

  /** Evaluates the health of the distributed system */
  private DistributedSystemHealthEvaluator eval;

  /** Notified when the health of the distributed system changes */
  private GemFireHealthImpl healthImpl;

  /** The number of seconds between health checks */
  private int interval;

  /** The thread in which the monitoring occurs */
  private Thread thread;

  /** Has this monitor been asked to stop? */
  private volatile boolean stopRequested = false;

  /** The health of the distributed system the last time we checked. */
  private GemFireHealth.Health prevHealth = GemFireHealth.GOOD_HEALTH;

  /**
   * The most recent <code>OKAY_HEALTH</code> diagnoses of the GemFire system
   */
  private List okayDiagnoses;

  /**
   * The most recent <code>POOR_HEALTH</code> diagnoses of the GemFire system
   */
  private List poorDiagnoses;

  ////////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>DistributedSystemHealthMonitor</code> that evaluates the health of the
   * distributed system against the given thresholds once every <code>interval</code> seconds.
   *
   * @param eval Used to evaluate the health of the distributed system
   * @param healthImpl Receives callbacks when the health of the distributed system changes
   * @param interval How often the health is checked
   */
  DistributedSystemHealthMonitor(DistributedSystemHealthEvaluator eval,
      GemFireHealthImpl healthImpl, int interval) {
    this.eval = eval;
    this.healthImpl = healthImpl;
    this.interval = interval;
    this.okayDiagnoses = new ArrayList();
    this.poorDiagnoses = new ArrayList();

    String name = String.format("Health monitor for %s",
        eval.getDescription());
    this.thread = new LoggingThread(name, this);
  }

  /**
   * Does the work of monitoring the health of the distributed system.
   */
  public void run() {
    if (logger.isDebugEnabled()) {
      logger.debug("Monitoring health of {} every {} seconds", this.eval.getDescription(),
          interval);
    }

    while (!this.stopRequested) {
      SystemFailure.checkFailure();
      try {
        Thread.sleep(interval * 1000);
        List status = new ArrayList();
        eval.evaluate(status);

        GemFireHealth.Health overallHealth = GemFireHealth.GOOD_HEALTH;
        this.okayDiagnoses.clear();
        this.poorDiagnoses.clear();

        for (Iterator iter = status.iterator(); iter.hasNext();) {
          AbstractHealthEvaluator.HealthStatus health =
              (AbstractHealthEvaluator.HealthStatus) iter.next();
          if (overallHealth == GemFireHealth.GOOD_HEALTH) {
            if ((health.getHealthCode() != GemFireHealth.GOOD_HEALTH)) {
              overallHealth = health.getHealthCode();
            }

          } else if (overallHealth == GemFireHealth.OKAY_HEALTH) {
            if (health.getHealthCode() == GemFireHealth.POOR_HEALTH) {
              overallHealth = GemFireHealth.POOR_HEALTH;
            }
          }

          GemFireHealth.Health healthCode = health.getHealthCode();
          if (healthCode == GemFireHealth.OKAY_HEALTH) {
            this.okayDiagnoses.add(health.getDiagnosis());

          } else if (healthCode == GemFireHealth.POOR_HEALTH) {
            this.poorDiagnoses.add(health.getDiagnosis());
            break;
          }
        }

        if (overallHealth != prevHealth) {
          healthImpl.healthChanged(this, overallHealth);
          this.prevHealth = overallHealth;
        }

      } catch (InterruptedException ex) {
        // We're all done
        // No need to reset the interrupted flag, since we're going to exit.
        break;
      }
    }

    eval.close();
    if (logger.isDebugEnabled()) {
      logger.debug("Stopped checking for distributed system health");
    }
  }

  /**
   * Starts this <code>DistributedSystemHealthMonitor</code>
   */
  void start() {
    this.thread.start();
  }

  /**
   * Stops this <code>DistributedSystemHealthMonitor</code>
   */
  void stop() {
    if (this.thread.isAlive()) {
      this.stopRequested = true;
      this.thread.interrupt();
      this.healthImpl.nodeLeft(null, this);

      try {
        this.thread.join();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        logger.warn("Interrupted while stopping health monitor thread",
            ex);
      }
    }
  }

  ////////////////////// GemFireVM Methods //////////////////////

  public java.net.InetAddress getHost() {
    try {
      return SocketCreator.getLocalHost();

    } catch (Exception ex) {
      throw new org.apache.geode.InternalGemFireException(
          "Could not get localhost?!");
    }
  }

  public String getName() {
    throw new UnsupportedOperationException("Not a real GemFireVM");
  }

  public java.io.File getWorkingDirectory() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public java.io.File getGeodeHomeDir() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public java.util.Date getBirthDate() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public Properties getLicenseInfo() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public GemFireMemberStatus getSnapshot() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public RegionSubRegionSnapshot getRegionSnapshot() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public StatResource[] getStats(String statisticsTypeName) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public StatResource[] getAllStats() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public DLockInfo[] getDistributedLockInfo() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public void addStatListener(StatListener observer, StatResource observedResource,
      Stat observedStat) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public void removeStatListener(StatListener observer) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public void addHealthListener(HealthListener observer, GemFireHealthConfig cfg) {

  }

  public void removeHealthListener() {

  }

  public void resetHealthStatus() {
    this.prevHealth = GemFireHealth.GOOD_HEALTH;
  }

  public String[] getHealthDiagnosis(GemFireHealth.Health healthCode) {
    if (healthCode == GemFireHealth.GOOD_HEALTH) {
      return new String[0];

    } else if (healthCode == GemFireHealth.OKAY_HEALTH) {
      String[] array = new String[this.okayDiagnoses.size()];
      this.okayDiagnoses.toArray(array);
      return array;

    } else {
      Assert.assertTrue(healthCode == GemFireHealth.POOR_HEALTH);
      String[] array = new String[this.poorDiagnoses.size()];
      this.poorDiagnoses.toArray(array);
      return array;
    }
  }

  public Config getConfig() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public void setConfig(Config cfg) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public GfManagerAgent getManagerAgent() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public String[] getSystemLogs() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public void setInspectionClasspath(String classpath) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public String getInspectionClasspath() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public Region[] getRootRegions() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public Region getRegion(CacheInfo c, String path) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public Region createVMRootRegion(CacheInfo c, String name, RegionAttributes attrs) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public Region createSubregion(CacheInfo c, String parentPath, String name,
      RegionAttributes attrs) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public void setCacheInspectionMode(int mode) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public int getCacheInspectionMode() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public void takeRegionSnapshot(String regionName, int snapshotId) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public InternalDistributedMember getId() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public CacheInfo getCacheInfo() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public String getVersionInfo() {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public CacheInfo setCacheLockTimeout(CacheInfo c, int v) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public CacheInfo setCacheLockLease(CacheInfo c, int v) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public CacheInfo setCacheSearchTimeout(CacheInfo c, int v) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public AdminBridgeServer addCacheServer(CacheInfo cache) throws AdminException {

    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public AdminBridgeServer getBridgeInfo(CacheInfo cache, int id) throws AdminException {

    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public AdminBridgeServer startBridgeServer(CacheInfo cache, AdminBridgeServer bridge)
      throws AdminException {

    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  public AdminBridgeServer stopBridgeServer(CacheInfo cache, AdminBridgeServer bridge)
      throws AdminException {

    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  /**
   * This operation is not supported for this object. Will throw UnsupportedOperationException if
   * invoked.
   */
  public void setAlertsManager(StatAlertDefinition[] alertDefs, long refreshInterval,
      boolean setRemotely) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  /**
   * This operation is not supported for this object. Will throw UnsupportedOperationException if
   * invoked.
   */
  public void setRefreshInterval(long refreshInterval) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }

  /**
   * This operation is not supported for this object. Will throw UnsupportedOperationException if
   * invoked.
   */
  public void updateAlertDefinitions(StatAlertDefinition[] alertDefs, int actionCode) {
    throw new UnsupportedOperationException(
        "Not a real GemFireVM");
  }
}
