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
package com.gemstone.gemfire.cache.client.internal.locator.wan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorMembershipListener;
import com.gemstone.gemfire.distributed.internal.WanLocatorDiscoverer;
import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorJoinMessage;
import com.gemstone.gemfire.cache.client.internal.locator.wan.RemoteLocatorJoinRequest;
import com.gemstone.gemfire.cache.client.internal.locator.wan.RemoteLocatorJoinResponse;
import com.gemstone.gemfire.cache.client.internal.locator.wan.RemoteLocatorPingRequest;
import com.gemstone.gemfire.cache.client.internal.locator.wan.RemoteLocatorPingResponse;
import com.gemstone.gemfire.cache.client.internal.locator.wan.RemoteLocatorRequest;
import com.gemstone.gemfire.cache.client.internal.locator.wan.RemoteLocatorResponse;
import com.gemstone.gemfire.internal.CopyOnWriteHashSet;
import com.gemstone.gemfire.internal.DSFIDFactory;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.CommandInitializer;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;

/**
 * An implementation of
 * {@link com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorMembershipListener}
 * 
 * @author kbachhav
 * 
 */
public class LocatorMembershipListenerImpl implements LocatorMembershipListener {

  private ConcurrentMap<Integer, Set<DistributionLocatorId>> allLocatorsInfo = new ConcurrentHashMap<Integer, Set<DistributionLocatorId>>();
  
  private ConcurrentMap<Integer, Set<String>> allServerLocatorsInfo = new ConcurrentHashMap<Integer, Set<String>>();
  
  private static final Logger logger = LogService.getLogger();
  
  private DistributionConfig config;
  
  private int port;
  
  public LocatorMembershipListenerImpl() {
  }
  
  public void setPort(int port){
    this.port = port;
  }

  public void setConfig(DistributionConfig config) {
    this.config = config;
  }
  
  /**
   * When the new locator is added to remote locator metadata, inform all other
   * locators in remote locator metadata about the new locator so that they can
   * update their remote locator metadata.
   * 
   * @param locator
   */
  
  public void locatorJoined(final int distributedSystemId,
      final DistributionLocatorId locator,
      final DistributionLocatorId sourceLocator) {
    Thread distributeLocator = new Thread(new Runnable() {
      public void run() {
        ConcurrentMap<Integer, Set<DistributionLocatorId>> remoteLocators = getAllLocatorsInfo();
        ArrayList<DistributionLocatorId> locatorsToRemove = new ArrayList<DistributionLocatorId>();
        
        String localLocator = config.getStartLocator();
        DistributionLocatorId localLocatorId = null;
        if (localLocator.equals(DistributionConfig.DEFAULT_START_LOCATOR)) {
          localLocatorId = new DistributionLocatorId(port, config
              .getBindAddress());
        }
        else {
          localLocatorId = new DistributionLocatorId(localLocator);
        }
        locatorsToRemove.add(localLocatorId);
        locatorsToRemove.add(locator);
        locatorsToRemove.add(sourceLocator);
        
        Map<Integer, Set<DistributionLocatorId>> localCopy = new HashMap<Integer, Set<DistributionLocatorId>>();
        for(Map.Entry<Integer, Set<DistributionLocatorId>> entry : remoteLocators.entrySet()){
          Set<DistributionLocatorId> value = new CopyOnWriteHashSet<DistributionLocatorId>(entry.getValue());
          localCopy.put(entry.getKey(), value);
        }  
        for(Map.Entry<Integer, Set<DistributionLocatorId>> entry : localCopy.entrySet()){
          for(DistributionLocatorId removeLocId : locatorsToRemove){
            if(entry.getValue().contains(removeLocId)){
              entry.getValue().remove(removeLocId);
            }
          }
          for (DistributionLocatorId value : entry.getValue()) {
            try {
              TcpClient.requestToServer(value.getHost(), value.getPort(),
                  new LocatorJoinMessage(distributedSystemId, locator, localLocatorId, ""), 1000, false);
            }
            catch (Exception e) {
              if (logger.isDebugEnabled()) {
                logger.debug(LocalizedMessage.create(LocalizedStrings.LOCATOR_MEMBERSHIP_LISTENER_COULD_NOT_EXCHANGE_LOCATOR_INFORMATION_0_1_WIHT_2_3, 
                    new Object[] { locator.getHost(), locator.getPort(), value.getHost(), value.getPort() }));
              }
            }
            try {
              TcpClient.requestToServer(locator.getHost(), locator.getPort(),
                  new LocatorJoinMessage(entry.getKey(), value, localLocatorId, ""), 1000, false);
            }
            catch (Exception e) {
              if (logger.isDebugEnabled()) {
                logger.debug(LocalizedMessage.create(LocalizedStrings.LOCATOR_MEMBERSHIP_LISTENER_COULD_NOT_EXCHANGE_LOCATOR_INFORMATION_0_1_WIHT_2_3,
                    new Object[] { value.getHost(), value.getPort(), locator.getHost(), locator.getPort() }));
              }
            }
          }
        }
      }
    });
    distributeLocator.setDaemon(true);
    distributeLocator.start();
  }

  public Object handleRequest(Object request) {
    Object response = null;
    if (request instanceof RemoteLocatorJoinRequest) {
      response = updateAllLocatorInfo((RemoteLocatorJoinRequest)request);
    }
    else if (request instanceof LocatorJoinMessage) {
      response = informAboutRemoteLocators((LocatorJoinMessage)request);
    }
    else if (request instanceof RemoteLocatorPingRequest) {
      response = getPingResponse((RemoteLocatorPingRequest)request);
    }
    else if (request instanceof RemoteLocatorRequest) {
      response = getRemoteLocators((RemoteLocatorRequest)request);
    }
    return response;
  }
  
  /**
   * A locator from the request is checked against the existing remote locator
   * metadata. If it is not available then added to existing remote locator
   * metadata and LocatorMembershipListener is invoked to inform about the
   * this newly added locator to all other locators available in remote locator
   * metadata. As a response, remote locator metadata is sent.
   * 
   * @param request
   */
  private synchronized Object updateAllLocatorInfo(RemoteLocatorJoinRequest request) {
    int distributedSystemId = request.getDistributedSystemId();
    DistributionLocatorId locator = request.getLocator();

    LocatorHelper.addLocator(distributedSystemId, locator, this, null);
    return new RemoteLocatorJoinResponse(this.getAllLocatorsInfo());
  }
  
  private Object getPingResponse(RemoteLocatorPingRequest request) {
   return new RemoteLocatorPingResponse();
  }
  
  private Object informAboutRemoteLocators(LocatorJoinMessage request){
    // TODO: FInd out the importance of list locatorJoinMessages. During
    // refactoring I could not understand its significance
//    synchronized (locatorJoinObject) {
//      if (locatorJoinMessages.contains(request)) {
//        return null;
//      }
//      locatorJoinMessages.add(request);  
//    }
    int distributedSystemId = request.getDistributedSystemId();
    DistributionLocatorId locator = request.getLocator();
    DistributionLocatorId sourceLocatorId = request.getSourceLocator();

    LocatorHelper.addLocator(distributedSystemId, locator, this, sourceLocatorId);
    return null;
  }
  
  private Object getRemoteLocators(RemoteLocatorRequest request) {
    int dsId = request.getDsId();
    Set<String> locators = this.getRemoteLocatorInfo(dsId);
    return new RemoteLocatorResponse(locators);
  }
  
  public Set<String> getRemoteLocatorInfo(int dsId) {
    return this.allServerLocatorsInfo.get(dsId);
  }

  public ConcurrentMap<Integer,Set<DistributionLocatorId>> getAllLocatorsInfo() {
    return this.allLocatorsInfo;
  }
  
  public ConcurrentMap<Integer,Set<String>> getAllServerLocatorsInfo() {
    return this.allServerLocatorsInfo;
  }
  
  public void clearLocatorInfo(){
    allLocatorsInfo.clear();
    allServerLocatorsInfo.clear();
  }
}
