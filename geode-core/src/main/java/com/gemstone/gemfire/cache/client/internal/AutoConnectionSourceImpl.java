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

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.client.NoAvailableLocatorsException;
import com.gemstone.gemfire.cache.client.internal.PoolImpl.PoolTask;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionResponse;
import com.gemstone.gemfire.cache.client.internal.locator.ClientReplacementRequest;
import com.gemstone.gemfire.cache.client.internal.locator.GetAllServersRequest;
import com.gemstone.gemfire.cache.client.internal.locator.GetAllServersResponse;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorListRequest;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorListResponse;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionResponse;
import com.gemstone.gemfire.cache.client.internal.locator.ServerLocationRequest;
import com.gemstone.gemfire.cache.client.internal.locator.ServerLocationResponse;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * A connection source which uses locators to find
 * the least loaded server.
 * @author dsmith
 * @since 5.7
 *
 */
public class AutoConnectionSourceImpl implements ConnectionSource {

  private static final Logger logger = LogService.getLogger();
  
  protected static final LocatorListRequest LOCATOR_LIST_REQUEST = new LocatorListRequest();
  private static final Comparator<InetSocketAddress> SOCKET_ADDRESS_COMPARATOR = new Comparator<InetSocketAddress>() {
    public int compare(InetSocketAddress o1,InetSocketAddress o2){
      //shouldn't happen, but if it does we'll say they're the same.
      if(o1.getAddress() == null || o2.getAddress() == null) {
        return 0;
      }
      
      int result = o1.getAddress().getCanonicalHostName().compareTo(o2.getAddress().getCanonicalHostName());
      if(result != 0) {
        return result;
      }
      
      else return o1.getPort() - o2.getPort();
    }
  };
  protected final List<InetSocketAddress> initialLocators;
  private final String serverGroup;
  private AtomicReference<LocatorList> locators = new AtomicReference<LocatorList>();
  protected InternalPool pool;
  private final int connectionTimeout;  
  private long pingInterval;
  private volatile LocatorDiscoveryCallback locatorCallback = new LocatorDiscoveryCallbackAdapter();
  private volatile boolean isBalanced = true;
  /**
   * key is the InetSocketAddress of the locator.
   * value will be an exception if we have already found the locator to be dead.
   * value will be null if we last saw him alive.
   */
  private final Map<InetSocketAddress,Exception> locatorState = new HashMap<InetSocketAddress,Exception>();
  
  /**
   * @param contacts
   * @param serverGroup
   * @param handshakeTimeout
   */
  public AutoConnectionSourceImpl(List<InetSocketAddress>contacts, String serverGroup, int handshakeTimeout) {
    ArrayList<InetSocketAddress> tmpContacts = new ArrayList<InetSocketAddress>(contacts);
    this.locators.set(new LocatorList(tmpContacts));
    this.initialLocators = Collections.unmodifiableList(tmpContacts);
    this.connectionTimeout = handshakeTimeout;
    this.serverGroup = serverGroup;
  }
  
  public boolean isBalanced() {
    return isBalanced;
  }

  public ServerLocation findReplacementServer(ServerLocation currentServer, Set/*<ServerLocation>*/ excludedServers) {
    if(PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return null;
    }
    ClientReplacementRequest request  = new ClientReplacementRequest(currentServer, excludedServers, serverGroup);
    ClientConnectionResponse response = (ClientConnectionResponse) queryLocators(request);
    if (response==null) {
      // why log a warning if we are going to throw the caller and exception?
      //getLogger().warning("Unable to connect to any locators in the list " + locators);
      throw new NoAvailableLocatorsException("Unable to connect to any locators in the list " + locators);
    }
//    if(getLogger().fineEnabled()) {
//      getLogger().fine("Received client connection response with server " + response.getServer());
//    }
    
    return response.getServer();
  }

  public ServerLocation findServer(Set excludedServers) {
    if(PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return null;
    }
    ClientConnectionRequest request  = new ClientConnectionRequest(excludedServers, serverGroup);
    ClientConnectionResponse response = (ClientConnectionResponse) queryLocators(request);
    if (response==null) {
      // why log a warning if we are going to throw the caller and exception?
      //getLogger().warning("Unable to connect to any locators in the list " + locators);
      throw new NoAvailableLocatorsException("Unable to connect to any locators in the list " + locators);
    }
//    if(getLogger().fineEnabled()) {
//      getLogger().fine("Received client connection response with server " + response.getServer());
//    }
    
    return response.getServer();
  }
  
  
  public ArrayList<ServerLocation> findAllServers() {
    if (PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return null;
    }
    GetAllServersRequest request = new GetAllServersRequest(serverGroup);
    GetAllServersResponse response = (GetAllServersResponse)queryLocators(request);
    if(response != null){
      return response.getServers();
    }else {
      return null ;
    }
  }
  
  public List/* ServerLocation */findServersForQueue(      
      Set/* <ServerLocation> */excludedServers, int numServers,
      ClientProxyMembershipID proxyId, boolean findDurableQueue) {
    if(PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return new ArrayList();
    }
    QueueConnectionRequest request  = new QueueConnectionRequest(proxyId,numServers,excludedServers, serverGroup,findDurableQueue);
    QueueConnectionResponse response = (QueueConnectionResponse) queryLocators(request);
    if (response==null) {
      // why log a warning if we are going to throw the caller and exception?
      //getLogger().warning("Unable to connect to any locators in the list " + locators);
      throw new NoAvailableLocatorsException("Unable to connect to any locators in the list " + locators);
    }
    //TODO - do this logic on the server side, return one list in the message.
    List result = response.getServers();
//    if(getLogger().fineEnabled()) {
//      getLogger().fine("Received queue connection response with server " + result+" excludeList:"+excludedServers);
//    }
    
    return result;
  }
  
  
  private ServerLocationResponse queryOneLocator(InetSocketAddress locator, ServerLocationRequest request) {
    InetAddress addr = locator.getAddress();
    int port = locator.getPort();
    Object returnObj = null;
    try {
      pool.getStats().incLocatorRequests();
      returnObj = TcpClient.requestToServer(addr, port, request, connectionTimeout);
      ServerLocationResponse response = (ServerLocationResponse)returnObj; 
      pool.getStats().incLocatorResponses();
      if(response != null) {
        reportLiveLocator(locator);
      }
      return response;
    } catch(IOException ioe) {
      reportDeadLocator(locator, ioe);
      return null;
    } catch (ClassNotFoundException e) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.AutoConnectionSourceImpl_RECEIVED_EXCEPTION_FROM_LOCATOR_0, locator), e);
      return null;    
    } catch (ClassCastException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Received odd response object from the locator: {}", returnObj);
      }
      reportDeadLocator(locator, e);
      return null;
    }
  }

  protected ServerLocationResponse queryLocators(ServerLocationRequest request) {
    Iterator controllerItr = locators.get().iterator();
    ServerLocationResponse response = null;
    
    final boolean isDebugEnabled = logger.isDebugEnabled();
    do {
      InetSocketAddress locator = (InetSocketAddress) controllerItr.next();
      if (isDebugEnabled) {
        logger.debug("Sending query to locator {}: {}", locator, request);
      }
      response = queryOneLocator(locator, request);
      if (isDebugEnabled) {
        logger.debug("Received query response from locator {}: {}", locator, response);
      }
    } while(controllerItr.hasNext() && (response == null || !response.hasResult()));
    
    if(response == null) {
      return null;
    }
    
    return response;
  }

  protected void updateLocatorList(LocatorListResponse response) {
    if (response == null) return;
    isBalanced = response.isBalanced();
    ArrayList<ServerLocation> locatorResponse = response.getLocators();

    ArrayList<InetSocketAddress> newLocators  = new ArrayList<InetSocketAddress>(locatorResponse.size());

    Set<InetSocketAddress> badLocators  = new HashSet<InetSocketAddress>(initialLocators);
    for(Iterator<ServerLocation> itr = locatorResponse.iterator(); itr.hasNext(); ) {
      ServerLocation locator =  itr.next();
      InetSocketAddress address = new InetSocketAddress(locator.getHostName(), locator.getPort());
      newLocators.add(address);
      badLocators.remove(address);
    }

    newLocators.addAll(badLocators);

    if(logger.isInfoEnabled()) {
      LocatorList oldLocators = (LocatorList) locators.get();
      ArrayList<InetSocketAddress> removedLocators  = new ArrayList<InetSocketAddress>(oldLocators.getLocators());
      removedLocators.removeAll(newLocators);

      ArrayList<InetSocketAddress> addedLocators  = new ArrayList<InetSocketAddress>(newLocators);
      addedLocators.removeAll(oldLocators.getLocators());
      if(!addedLocators.isEmpty()) {
        locatorCallback.locatorsDiscovered(Collections.unmodifiableList(addedLocators));
        logger.info(LocalizedMessage.create(LocalizedStrings.AutoConnectionSourceImpl_AUTOCONNECTIONSOURCE_DISCOVERED_NEW_LOCATORS_0, addedLocators));
      }
      if(!removedLocators.isEmpty()) {
        locatorCallback.locatorsRemoved(Collections.unmodifiableList(removedLocators));
        logger.info(LocalizedMessage.create(LocalizedStrings.AutoConnectionSourceImpl_AUTOCONNECTIONSOURCE_DROPPING_PREVIOUSLY_DISCOVERED_LOCATORS_0, removedLocators));
      }
    }
    LocatorList newLocatorList = new LocatorList(newLocators);
    locators.set(newLocatorList);
    pool.getStats().setLocatorCount(newLocators.size());
  }

  public void start(InternalPool pool) {
    this.pool = pool;
    pool.getStats().setInitialContacts(((LocatorList)locators.get()).size());
    this.pingInterval = pool.getPingInterval();
    
    pool.getBackgroundProcessor().scheduleWithFixedDelay(new UpdateLocatorListTask(), 0, pingInterval, TimeUnit.MILLISECONDS);
  }

  public void stop() {
    
  }
  
  public void setLocatorDiscoveryCallback(LocatorDiscoveryCallback callback) {
    this.locatorCallback = callback;
  }
  
  private synchronized void reportLiveLocator(InetSocketAddress l) {
    Object prevState = this.locatorState.put(l, null);
    if (prevState != null) {
      logger.info(LocalizedMessage.create(LocalizedStrings.AutoConnectionSourceImpl_COMMUNICATION_HAS_BEEN_RESTORED_WITH_LOCATOR_0, l));
    }
  }
  
  private synchronized void reportDeadLocator(InetSocketAddress l, Exception ex) {
    Object prevState = this.locatorState.put(l, ex);
    if (prevState == null) {
      if (ex instanceof ConnectException) {
        logger.info(LocalizedMessage.create(LocalizedStrings.AutoConnectionSourceImpl_LOCATOR_0_IS_NOT_RUNNING, l),ex);
      } else {
        logger.info(LocalizedMessage.create(
            LocalizedStrings.AutoConnectionSourceImpl_COMMUNICATION_WITH_LOCATOR_0_FAILED_WITH_1,
            new Object[] {l, ex}),ex);
      }
    }
  }
  
  /** A list of locators, which
   * remembers the last known good locator.
   */
  private static class LocatorList {
    protected final List<InetSocketAddress> locators;
    protected AtomicInteger currentLocatorIndex = new AtomicInteger();
    
    public LocatorList(List<InetSocketAddress> locators) {
      Collections.sort(locators, SOCKET_ADDRESS_COMPARATOR);
      this.locators = Collections.unmodifiableList(locators);
    }
    
    public Collection<InetSocketAddress> getLocators() {
      return locators;
    }

    public int size() {
      return locators.size();
    }

    public Iterator<InetSocketAddress> iterator() {
      return new LocatorIterator();
    }
    
    @Override
    public String toString() {
      return locators.toString();
    }
    
    
    /**
     * An iterator which iterates all of the controllers,
     * starting at the last known good controller.
     * 
     */
    protected class LocatorIterator implements Iterator<InetSocketAddress> {
      private int startLocator= currentLocatorIndex.get();
      private int locatorNum = 0;
      
      public boolean hasNext() {
        return locatorNum < locators.size();
      }
      
      public InetSocketAddress next() {
        if(!hasNext()) {
          return null;
        } else {
          int index = (locatorNum + startLocator) % locators.size();
          InetSocketAddress nextLocator = locators.get(index);
          currentLocatorIndex.set(index);
          locatorNum++;
          return nextLocator;
        }
      }
      
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }
  }
  
  protected class UpdateLocatorListTask extends PoolTask {
    @Override
    public void run2() {
      if(pool.getCancelCriterion().cancelInProgress() != null) {
        return;
      }
      LocatorListResponse response = (LocatorListResponse) queryLocators(LOCATOR_LIST_REQUEST);
      updateLocatorList(response);
    }
  }
}
