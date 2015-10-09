/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.server.internal;

import java.util.ArrayList;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.client.internal.CacheServerLoadMessage;
import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.cache.server.ServerLoadProbe;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.internal.cache.CacheServerAdvisor;
import com.gemstone.gemfire.internal.cache.tier.Acceptor;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ConnectionListener;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * A class which monitors the load on a bridge server and 
 * periodically sends updates to the locator.
 * 
 * @author dsmith
 * @since 5.7
 *
 */
public class LoadMonitor implements ConnectionListener {
  private static final Logger logger = LogService.getLogger();
  
  private final ServerLoadProbe probe;
  private final ServerMetricsImpl metrics;
  protected final CacheServerAdvisor advisor;
  protected ServerLocation location;
  private final PollingThread pollingThread;
  protected volatile ServerLoad lastLoad;
  protected CacheServerStats stats;

  public LoadMonitor(ServerLoadProbe probe, int maxConnections,
      long pollInterval, int forceUpdateFrequency, CacheServerAdvisor advisor) {
    this.probe = probe;
    this.metrics = new ServerMetricsImpl(maxConnections);
    this.pollingThread = new PollingThread(pollInterval, forceUpdateFrequency);
    lastLoad = getLoad();
    this.advisor = advisor;
  }
 
  /**
   * Start the load monitor. Starts the background thread which
   * polls the load monitor and sends updates about load.
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
      logger.warn(LocalizedMessage.create(LocalizedStrings.LoadMonitor_INTERRUPTED_WAITING_FOR_POLLING_THREAD_TO_FINISH));
      Thread.currentThread().interrupt();
    }
    probe.close();
  }

  public void connectionClosed(boolean lastConnection, byte communicationMode) {
    if(communicationMode == Acceptor.CLIENT_TO_SERVER) {
      metrics.decConnectionCount();
    }
    if(lastConnection) {
      metrics.decClientCount();
    }
  }
  
  public ServerLoad getLastLoad() {
    return lastLoad;
  }

  public void connectionOpened(boolean firstConnection, byte communicationMode) {
    //ignore all other types of client connections.
    if(communicationMode == Acceptor.CLIENT_TO_SERVER) {
      metrics.incConnectionCount();
    }
    if(firstConnection) {
      metrics.incClientCount();
    }
  }

  /**
   * Keeps track of the clients that have added a queue since the
   * last load was sent to the server-locator.
   * @since 5.7
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
    if(load == null) {
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
      super("BridgeServer-LoadPollingThread");
      this.pollInterval = pollInterval;
      this.forceUpdateFrequency = forceUpdateFrequency;
      this.setDaemon(true);
    }
    
    public void close() {
      alive = false;
      synchronized(signal) {
        signal.notifyAll();
      }
    }

    @Override
    public void run() {
      while(alive) {
        try {
          synchronized(signal) {
            long end = System.currentTimeMillis() + pollInterval;
            long remaining = pollInterval;
            while(alive && remaining > 0) {
              signal.wait(remaining);
              remaining = end - System.currentTimeMillis();
            }
          }
          
          if(!alive) {
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
          if (!previousLoad.equals(load)
              || myClientIds != null
              || ++skippedLoadUpdates > forceUpdateFrequency) {
            Set locators = advisor.adviseControllers();
            
            if (logger.isDebugEnabled()) {
              logger.debug("Bridge Server Load Monitor Transmitting load {} to locators {}", load, locators);
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
              //Update any local locators
              message.updateLocalLocators();
            }
            skippedLoadUpdates = 0;
          }
          else {
            if (logger.isDebugEnabled()) {
              logger.debug("Bridge Server Load Monitor Load {} hasn't changed, not transmitting. skippedLoadUpdates={}", load, skippedLoadUpdates);
            }
          }
        }
        catch(InterruptedException e) {
          SystemFailure.checkFailure();
        }
        catch(VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (CancelException e) {
          return;
        }
        catch(Throwable t) {
          SystemFailure.checkFailure();
          logger.warn(LocalizedMessage.create(LocalizedStrings.LoadMonitor_CACHESERVER_LOAD_MONITOR_ERROR_IN_POLLING_THREAD), t);
        }
      } // while
    }
  }
}
