/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerQueueStatus;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.security.GemFireSecurityException;

/**
 * A connection source where the list of endpoints is specified explicitly. 
 * @author dsmith
 * @since 5.7
 * 
 * TODO - the UnusedServerMonitor basically will force the pool to
 * have at least one connection to each server. Maybe we need to have it
 * create connections that are outside the pool?
 *
 */
public class ExplicitConnectionSourceImpl implements ConnectionSource {

  private static final Logger logger = LogService.getLogger();
  
  private List serverList;
  private int nextServerIndex = 0;
  private int nextQueueIndex = 0;
  private InternalPool pool;
  
  /**
   * A debug flag, which can be toggled by tests to disable/enable shuffling of
   * the endpoints list
   */
  private boolean DISABLE_SHUFFLING = Boolean
      .getBoolean("gemfire.bridge.disableShufflingOfEndpoints");

  public ExplicitConnectionSourceImpl(List/*<InetSocketAddress>*/contacts) {
    ArrayList serverList = new ArrayList(contacts.size());
    for(int i = 0; i < contacts.size(); i++) {
      InetSocketAddress addr = (InetSocketAddress)contacts.get(i);
      serverList.add(new ServerLocation(addr.getHostName(), addr.getPort()));
    }
    shuffle(serverList);
    this.serverList = Collections.unmodifiableList(serverList);
  }

  @Override
  public synchronized void start(InternalPool pool) {
    this.pool = pool;
    pool.getStats().setInitialContacts(serverList.size());
  }
  
  @Override
  public void stop() {
    //do nothing
  }

  @Override
  public ServerLocation findReplacementServer(ServerLocation currentServer, Set/*<ServerLocation>*/ excludedServers) {
    // at this time we always try to find a server other than currentServer
    // and if we do return it. Otherwise return null;
    // @todo grid: We could add balancing information to the explicit source
    // so that clients would attempt to keep the same number of connections
    // to each server but it would be a bit of work.
    // Plus we need to make sure it would work ok for hardware load balancers.
    HashSet excludedPlusCurrent = new HashSet(excludedServers);
    excludedPlusCurrent.add(currentServer);
    return findServer(excludedPlusCurrent);
  }
  
  @Override
  public synchronized ServerLocation findServer(Set excludedServers) {
    if(PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return null;
    }
    ServerLocation nextServer;
    int startIndex = nextServerIndex;
    do {
      nextServer = (ServerLocation) serverList.get(nextServerIndex);
      if(++nextServerIndex >= serverList.size()) {
        nextServerIndex = 0;
      }
      if(!excludedServers.contains(nextServer)) {
        return nextServer;
      }
    } while(nextServerIndex != startIndex);
    
    return null;
  }
  
  /**
   * TODO - this algorithm could be cleaned up. Right now we have to
   * connect to every server in the system to find where our durable
   * queue lives.
   */
  @Override
  public synchronized List findServersForQueue(Set excludedServers,
      int numServers, ClientProxyMembershipID proxyId, boolean findDurableQueue) {
    if(PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return new ArrayList();
    }
    if(numServers == -1) {
      numServers = Integer.MAX_VALUE;
    }
    if(findDurableQueue && proxyId.isDurable()) {
      return findDurableQueues(excludedServers, numServers);
    } else {
      return pickQueueServers(excludedServers, numServers);
    }
  }
  
  @Override
  public boolean isBalanced() {
    return false;
  }
  
  private List pickQueueServers(Set excludedServers,
      int numServers) {
    
    ArrayList result = new ArrayList();
    ServerLocation nextQueue;
    int startIndex = nextQueueIndex;
    do {
      nextQueue= (ServerLocation) serverList.get(nextQueueIndex);
      if(++nextQueueIndex >= serverList.size()) {
        nextQueueIndex = 0;
      }
      if(!excludedServers.contains(nextQueue)) {
        result.add(nextQueue);
      }
    } while(nextQueueIndex != startIndex && result.size() < numServers);
    
    return result;
  }

  /**
   * a "fake" operation which just extracts the queue status from the connection
   */
  private static class HasQueueOp implements Op {
    public static final HasQueueOp SINGLETON = new HasQueueOp();
    public Object attempt(Connection cnx) throws Exception {
      ServerQueueStatus status = cnx.getQueueStatus();
      return status.isNonRedundant() ? Boolean.FALSE : Boolean.TRUE;
    }
    @Override
    public boolean useThreadLocalConnection() {
      return false;
    }
  }
  
  private List findDurableQueues(Set excludedServers,
      int numServers) {
    ArrayList durableServers = new ArrayList();
    ArrayList otherServers = new ArrayList();
    
    logger.debug("ExplicitConnectionSource - looking for durable queue");
    
    for(Iterator itr = serverList.iterator(); itr.hasNext(); ) {
      ServerLocation server = (ServerLocation) itr.next();
      if(excludedServers.contains(server)) {
        continue;
      }
      
      //the pool will automatically create a connection to this server
      //and store it for future use.
      Boolean hasQueue;
      try {
        hasQueue = (Boolean) pool.executeOn(server, HasQueueOp.SINGLETON);
      } catch(GemFireSecurityException e) {
        throw e;
      } catch(Exception e) {
        if(e.getCause() instanceof GemFireSecurityException) {
          throw (GemFireSecurityException)e.getCause();
        }
        if(logger.isDebugEnabled()) {
          logger.debug("Unabled to check for durable queue on server {}: {}", server, e);
        }
        continue;
      }
      if(hasQueue != null) {
        if(hasQueue.booleanValue()) {
          if(logger.isDebugEnabled()) {
            logger.debug("Durable queue found on {}", server);
          }
          durableServers.add(server);
        } else {
          if(logger.isDebugEnabled()) {
            logger.debug("Durable queue was not found on {}", server);
          }
          otherServers.add(server);
        }
      }
    }

    int remainingServers = numServers - durableServers.size();
    if(remainingServers > otherServers.size()) {
      remainingServers = otherServers.size();
    }
    //note, we're always prefering the servers in the beginning of the list
    //but that's ok because we already shuffled the list in our constructor.
    if(remainingServers > 0) {
      durableServers.addAll(otherServers.subList(0, remainingServers));
      nextQueueIndex = remainingServers % serverList.size();
    }
    
    if(logger.isDebugEnabled()) {
      logger.debug("found {} servers out of {}", durableServers.size(), numServers);
    }
    
    return durableServers;
  }
  
  private void shuffle(List endpoints)
  {
    //this check was copied from ConnectionProxyImpl
    if (endpoints.size() < 2 || DISABLE_SHUFFLING) {
      /*
       * It is not safe to shuffle an ArrayList of size 1
       * java.lang.IndexOutOfBoundsException: Index: 1, Size: 1 at
       * java.util.ArrayList.RangeCheck(Unknown Source) at
       * java.util.ArrayList.get(Unknown Source) at
       * java.util.Collections.swap(Unknown Source) at
       * java.util.Collections.shuffle(Unknown Source)
       */
      return;
    }
    Collections.shuffle(endpoints);
  }
  
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("EndPoints[");
    synchronized(this) {
      Iterator it = serverList.iterator();
      while(it.hasNext()) {
        ServerLocation loc = (ServerLocation)it.next();
        sb.append(loc.getHostName()+":"+loc.getPort());
        if(it.hasNext()) {
          sb.append(",");
        }
      }
    }
    sb.append("]");
    return sb.toString();
  }
  
  ArrayList<ServerLocation> getAllServers() {
    ArrayList<ServerLocation> list = new ArrayList<ServerLocation>();
    list.addAll(this.serverList);
    return list;
  }
}
