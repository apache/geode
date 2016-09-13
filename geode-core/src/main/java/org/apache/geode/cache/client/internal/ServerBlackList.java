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
package org.apache.geode.cache.client.internal;

import org.apache.geode.cache.client.internal.PoolImpl.PoolTask;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is designed to prevent the client from spinning
 * and reconnected to the same failed server over and over.
 * We've removed the old dead server monitor code because
 * the locator is supposed to keep track of what servers are
 * alive or dead. However, there is still the possibility that the locator
 * may tell us a server is alive but we are unable to reach it. 
 * 
 * This class keeps track of the number of consecutive failures
 * that happen to on each server. If the number of failures exceeds the limit,
 * the server is added to a blacklist for a certain period of time. After
 * the time is expired, the server comes off the blacklist, but the next
 * failure will put the server back on the list for a longer period of time.
 * 
 *
 */
public class ServerBlackList {
  
  private static final Logger logger = LogService.getLogger();
  
  private final Map/*<ServerLocation, AI>*/ failureTrackerMap = new HashMap();
  protected final Set blacklist = new CopyOnWriteArraySet();
  private final Set unmodifiableBlacklist = Collections.unmodifiableSet(blacklist);
  protected ScheduledExecutorService background;
  protected final ListenerBroadcaster broadcaster = new ListenerBroadcaster();
  
  //not final for tests.
  static int THRESHOLD = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "ServerBlackList.THRESHOLD", 3).intValue();
  protected final long pingInterval;
  
  public ServerBlackList(long pingInterval) {
    this.pingInterval = pingInterval;
  }
  
  public void start(ScheduledExecutorService background) {
    this.background = background;
  }
  
  FailureTracker getFailureTracker(ServerLocation location) {
    FailureTracker failureTracker;
    synchronized(failureTrackerMap) {
      failureTracker = (FailureTracker) failureTrackerMap.get(location);
      if(failureTracker == null) {
        failureTracker = new FailureTracker(location);
        failureTrackerMap.put(location, failureTracker);
      }
    }
    
    return failureTracker;
  }
  
  public Set getBadServers() {
    return unmodifiableBlacklist;
  }
  
  public class FailureTracker {
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private final ServerLocation location;
    
    public FailureTracker(ServerLocation location) {
      this.location = location;
    }
    
    public void reset() {
      consecutiveFailures.set(0);
    }
    
    public void addFailure() {
      if(blacklist.contains(location)) {
        //A second failure must have happened before we added
        //this server to the blacklist. Don't count that failure.
        return;
      }
      long failures = consecutiveFailures.incrementAndGet();
      if(failures >= THRESHOLD) {
        if(logger.isDebugEnabled()) {
          logger.debug("Blacklisting server {} for {}ms because it had {} consecutive failures", location, pingInterval, failures);
        }
        blacklist.add(location);
        broadcaster.serverAdded(location);
        try {
          background.schedule(new ExpireBlackListTask(location), pingInterval, TimeUnit.MILLISECONDS);
        } catch(RejectedExecutionException e) {
          //ignore, the timer has been cancelled, which means we're shutting down.
        }
        
      }
    }
  }
  
  public void addListener(BlackListListener blackListListener) {
    broadcaster.listeners.add(blackListListener);
  }
  
  public void removeListener(BlackListListener blackListListener) {
    broadcaster.listeners.remove(blackListListener);
  }
  

  
  private class ExpireBlackListTask extends PoolTask {
    private ServerLocation location;

    public ExpireBlackListTask(ServerLocation location) {
      this.location = location;
    }
    
    @Override
    public void run2() {
      if(logger.isDebugEnabled()) {
        logger.debug("{} is no longer blacklisted", location);
      }
      blacklist.remove(location);
      broadcaster.serverRemoved(location);
    }
  }
  
  public static interface BlackListListener { 
    public void serverAdded(ServerLocation location);
    
    public void serverRemoved(ServerLocation location);
  }
  
  public static class BlackListListenerAdapter implements BlackListListener {
    public void serverAdded(ServerLocation location) {
      //do nothing
    }

    public void serverRemoved(ServerLocation location) {
      //do nothing      
    }
  }
  
  protected static class ListenerBroadcaster implements BlackListListener {
    
    protected Set listeners = new CopyOnWriteArraySet();

    public void serverAdded(ServerLocation location) {
      for(Iterator itr = listeners.iterator(); itr.hasNext(); ) {
        BlackListListener listener = (BlackListListener) itr.next();
        listener.serverAdded(location);
      }
    }

    public void serverRemoved(ServerLocation location) {
      for(Iterator itr = listeners.iterator(); itr.hasNext(); ) {
        BlackListListener listener = (BlackListListener) itr.next();
        listener.serverRemoved(location);
      }
    }
  }
}
