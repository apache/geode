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
package org.apache.geode.cache.server.internal;

import java.util.ArrayList;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.client.internal.CacheServerLoadMessage;
import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.cache.server.ServerLoadProbe;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.internal.cache.CacheServerAdvisor;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ConnectionListener;
import org.apache.geode.internal.logging.LogService;

/**
 * A class which monitors the load on a cache server and periodically sends updates to the locator.
 *
 * @since GemFire 5.7
 *
 */
public class LoadMonitor implements ConnectionListener {
  private static final Logger logger = LogService.getLogger();

  private final ServerLoadProbe probe;
  protected final ServerMetricsImpl metrics;
  protected final CacheServerAdvisor advisor;
  protected ServerLocation location;
  private final PollingThread pollingThread;
  protected volatile ServerLoad lastLoad;
  protected CacheServerStats stats;

  public LoadMonitor(ServerLoadProbe probe, int maxConnections, long pollInterval,
      int forceUpdateFrequency, CacheServerAdvisor advisor) {
    this.probe = probe;
    this.metrics = new ServerMetricsImpl(maxConnections);
    this.pollingThread = new PollingThread(pollInterval, forceUpdateFrequency);
    lastLoad = getLoad();
    this.advisor = advisor;
  }

  /**
   * Start the load monitor. Starts the background thread which polls the load monitor and sends
   * updates about load.
   */
  public void start(ServerLocation location, CacheServerStats cacheServerStats) {
    probe.open();
    this.location = location;
    this.pollingThread.start();
    this.stats = cacheServerStats;
    this.stats.setLoad(lastLoad);
  }

  /**
   * Stops the load monitor
   */
  public void stop() {
    this.pollingThread.close();
    try {
      this.pollingThread.join(5000);
    } catch (InterruptedException e) {
      logger.warn("Interrupted waiting for polling thread to finish");
      Thread.currentThread().interrupt();
    }
    probe.close();
  }

  public void connectionClosed(boolean lastConnection, CommunicationMode communicationMode) {
    if (communicationMode.isClientOperations()) {
      metrics.decConnectionCount();
    }
    if (lastConnection) {
      metrics.decClientCount();
    }
  }

  public ServerLoad getLastLoad() {
    return lastLoad;
  }

  public void connectionOpened(boolean firstConnection, CommunicationMode communicationMode) {
    if (communicationMode.isClientOperations()) {
      metrics.incConnectionCount();
    }
    if (firstConnection) {
      metrics.incClientCount();
    }
  }

  /**
   * Keeps track of the clients that have added a queue since the last load was sent to the
   * server-locator.
   *
   * @since GemFire 5.7
   */
  protected final ArrayList clientIds = new ArrayList();

  public void queueAdded(ClientProxyMembershipID id) {
    synchronized (this.clientIds) {
      metrics.incQueueCount();
      this.clientIds.add(id);
    }
  }

  public void queueRemoved() {
    metrics.decQueueCount();
  }

  protected ServerLoad getLoad() {
    ServerLoad load = this.probe.getLoad(metrics);
    if (load == null) {
      load = new ServerLoad();
    }
    return load;
  }

  private class PollingThread extends Thread {
    private final Object signal = new Object();
    private final long pollInterval;
    private volatile boolean alive = true;
    private final int forceUpdateFrequency;
    private int skippedLoadUpdates;

    public PollingThread(long pollInterval, int forceUpdateFrequency) {
      super("Cache Server Load Polling Thread");
      this.pollInterval = pollInterval;
      this.forceUpdateFrequency = forceUpdateFrequency;
      this.setDaemon(true);
    }

    public void close() {
      alive = false;
      synchronized (signal) {
        signal.notifyAll();
      }
    }

    @Override
    public void run() {
      while (alive) {
        try {
          synchronized (signal) {
            long end = System.currentTimeMillis() + pollInterval;
            long remaining = pollInterval;
            while (alive && remaining > 0) {
              signal.wait(remaining);
              remaining = end - System.currentTimeMillis();
            }
          }

          if (!alive) {
            return;
          }

          ServerLoad previousLoad = lastLoad;
          ArrayList myClientIds = null;
          ServerLoad load = null;
          synchronized (clientIds) {
            if (!clientIds.isEmpty()) {
              myClientIds = new ArrayList(clientIds);
              clientIds.clear();
            }
            load = getLoad();
          }
          lastLoad = load;

          // don't send a message if the load hasn't
          // changed, unless we have waited too long
          // since the last update.
          if (!previousLoad.equals(load) || myClientIds != null
              || ++skippedLoadUpdates > forceUpdateFrequency) {
            Set locators = advisor.adviseControllers();

            if (logger.isDebugEnabled()) {
              logger.debug("cache server Load Monitor Transmitting load {} to locators {}", load,
                  locators);
            }

            stats.setLoad(load);
            if (locators != null) {
              CacheServerLoadMessage message =
                  new CacheServerLoadMessage(load, location, myClientIds);
              message.setRecipients(locators);
              MembershipManager mgr = advisor.getDistributionManager().getMembershipManager();
              if (mgr == null || !mgr.isBeingSick()) { // test hook
                advisor.getDistributionManager().putOutgoing(message);
              }
              // Update any local locators
              message.updateLocalLocators();
            }
            skippedLoadUpdates = 0;
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "cache server Load Monitor Load {} hasn't changed, not transmitting. skippedLoadUpdates={}",
                  load, skippedLoadUpdates);
            }
          }
        } catch (InterruptedException e) {
          SystemFailure.checkFailure();
        } catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        } catch (CancelException e) {
          return;
        } catch (Throwable t) {
          SystemFailure.checkFailure();
          logger.warn("CacheServer Load Monitor Error in polling thread",
              t);
        }
      } // while
    }
  }
}
