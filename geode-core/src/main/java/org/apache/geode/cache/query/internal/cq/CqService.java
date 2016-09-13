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
package org.apache.geode.cache.query.internal.cq;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.internal.InternalPool;
import org.apache.geode.cache.client.internal.QueueManager;
import org.apache.geode.cache.client.internal.UserAttributes;
import org.apache.geode.cache.query.CqAttributes;
import org.apache.geode.cache.query.CqClosedException;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqExistsException;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.CqServiceStatistics;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.FilterRoutingInfo;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

public interface CqService {

  /**
   * Constructs a new named continuous query, represented by an instance of
   * CqQuery. The CqQuery is not executed, however, until the execute method
   * is invoked on the CqQuery. The name of the query will be used
   * to identify this query in statistics archival.
   *
   * @param cqName the String name for this query
   * @param queryString the OQL query
   * @param cqAttributes the CqAttributes
   * @param isDurable true if the CQ is durable 
   * @return the newly created CqQuery object
   * @throws CqExistsException if a CQ by this name already exists on this
   * client
   * @throws IllegalArgumentException if queryString or cqAttr is null
   * @throws IllegalStateException if this method is called from a cache
   *         server
   * @throws QueryInvalidException if there is a syntax error in the query
   * @throws CqException if failed to create cq, failure during creating  
   *         managing cq metadata info.
   *   E.g.: Query string should refer only one region, join not supported.
   *         The query must be a SELECT statement.
   *         DISTINCT queries are not supported.
   *         Projections are not supported.
   *         Only one iterator in the FROM clause is supported, and it must be a region path. 
   *         Bind parameters in the query are not supported for the initial release.
   *
   */
  public abstract ClientCQ newCq(String cqName, String queryString,
      CqAttributes cqAttributes, InternalPool pool, boolean isDurable)
      throws QueryInvalidException, CqExistsException, CqException;

  /**
   * Retrieve a cq by client cq name from server
   * @return the CqQuery or null if not found
   */
  public abstract CqQuery getClientCqFromServer(
      ClientProxyMembershipID clientProxyId, String clientCqName);

  /**
   * Retrieve a CqQuery by name.
   * @return the CqQuery or null if not found
   */
  public abstract InternalCqQuery getCq(String cqName);

  /**
   * Retrieve  all registered CQs
   */
  public abstract Collection<? extends InternalCqQuery> getAllCqs();

  /**
   * Retruns all the cqs on a given region.
   * 
   */
  public abstract Collection<? extends InternalCqQuery> getAllCqs(String regionName) throws CqException;

  /**
   * Executes all the cqs on this client.
   */
  public abstract void executeAllClientCqs() throws CqException;

  /**
   * Executes all the cqs on a given region.
   */
  public abstract void executeAllRegionCqs(String regionName)
      throws CqException;

  /**
   * Executes all the given cqs.
   */
  public abstract void executeCqs(Collection<? extends InternalCqQuery> cqs) throws CqException;

  /**
   * Stops all the cqs on a given region.
   */
  public abstract void stopAllClientCqs() throws CqException;

  /**
   * Stops all the cqs on a given region.
   */
  public abstract void stopAllRegionCqs(String regionName) throws CqException;

  /**
   * Stops all the specified cqs.
   */
  public abstract void stopCqs(Collection<? extends InternalCqQuery> cqs) throws CqException;

  /**
   * Closes all the cqs on a given region.
   */
  public abstract void closeCqs(String regionName) throws CqException;

  /**
   * Called directly on server side.
   * @param cqName
   * @param clientProxyId
   * @throws CqException
   */
  public abstract void closeCq(String cqName,
      ClientProxyMembershipID clientProxyId) throws CqException;

  public abstract void closeAllCqs(boolean clientInitiated);

  public abstract void closeAllCqs(boolean clientInitiated, Collection<? extends InternalCqQuery> cqs,
      boolean keepAlive);

  /**
   * Get statistics information for all CQs
   * @return the CqServiceStatistics
   */
  public abstract CqServiceStatistics getCqStatistics();

  /**
   * Server side method.
   * @param clientProxyId
   * @throws CqException
   */
  public abstract void closeClientCqs(ClientProxyMembershipID clientProxyId)
      throws CqException;

  /**
   * Returns all the CQs registered by the client.
   * @param clientProxyId
   * @return CQs registered by the client.
   */
  public abstract List<ServerCQ> getAllClientCqs(
      ClientProxyMembershipID clientProxyId);

  /**
   * Returns all the durable client CQs registered by the client.
   * @param clientProxyId
   * @return CQs registered by the client.
   */
  public abstract List<String> getAllDurableClientCqs(
      ClientProxyMembershipID clientProxyId) throws CqException;

  /**
   * Invokes the CqListeners for the given CQs.
   * @param cqs list of cqs with the cq operation from the Server.
   * @param messageType base operation
   * @param key 
   * @param value
   */
  public abstract void dispatchCqListeners(HashMap<String, Integer> cqs, int messageType,
      Object key, Object value, byte[] delta, QueueManager qManager,
      EventID eventId);

  public abstract void processEvents(CacheEvent event, Profile localProfile,
      Profile[] profiles, FilterRoutingInfo frInfo) throws CqException;
  
  public UserAttributes getUserAttributes(String cqName);
  
  /**
   * Closes the CqService.
   */
  public void close();
  
  /**
   * Returns true if the CQ service has not been closed yet.
   */
  public boolean isRunning();

  public void start();
  
  /**
   * @return Returns the serverCqName.
   */
  public String constructServerCqName(String cqName, ClientProxyMembershipID clientProxyId);
  
  /**
   * Called directly on server side.
   * @param cqName
   * @param clientId
   * @throws CqException
   */
  public void stopCq(String cqName, ClientProxyMembershipID clientId) throws CqException;
  
  /**
   * Called directly on the server side.
   * @param cqState new state
   * @param cQuery
   */
  public void resumeCQ(int cqState, ServerCQ cQuery);
  
  public void cqsDisconnected(Pool pool);
  
  public void cqsConnected(Pool pool);

  /**
   * Executes the given CqQuery, if the CqQuery for that name is not there
   * it registers the one and executes. This is called on the Server. 
   * @param cqName
   * @param queryString
   * @param cqState
   * @param clientProxyId
   * @param ccn
   * @param manageEmptyRegions whether to update the 6.1 emptyRegions map held in the CCN
   * @param regionDataPolicy the data policy of the region associated with the
   *        query.  This is only needed if manageEmptyRegions is true.
   * @param emptyRegionsMap map of empty regions.    
   * @throws IllegalStateException if this is called at client side.
   * @throws CqException
   */
  public ServerCQ executeCq(String cqName, String queryString,
      int cqState, ClientProxyMembershipID clientProxyId, 
      CacheClientNotifier ccn, boolean isDurable, boolean manageEmptyRegions,
      int regionDataPolicy, Map emptyRegionsMap) 
    throws CqException, RegionNotFoundException, CqClosedException;

  /**
   * Server side method.
   * Closes non-durable CQs for the given client proxy id.
   * @param clientProxyId
   * @throws CqException
   */
  public void closeNonDurableClientCqs(ClientProxyMembershipID clientProxyId)
      throws CqException;

  public abstract List<String> getAllDurableCqsFromServer(InternalPool pool);
}
