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
package org.apache.geode.cache.query.internal.cq;

import org.apache.geode.cache.query.CqClosedException;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

public interface ServerCQ extends InternalCqQuery {

  /**
   * @return the filterID
   */
  Long getFilterID();

  /**
   * @param filterID the filterID to set
   */
  void setFilterID(Long filterID);

  /**
   * Register the Query on server.
   *
   */
  void registerCq(ClientProxyMembershipID p_clientProxyId, CacheClientNotifier p_ccn, int p_cqState)
      throws CqException, RegionNotFoundException;

  /**
   * Adds into the CQ Results key cache.
   *
   */
  void addToCqResultKeys(Object key);

  /**
   * Removes the key from CQ Results key cache.
   *
   * @param isTokenMode if true removes the key if its in destroy token mode if false removes the
   *        key without any check.
   */
  void removeFromCqResultKeys(Object key, boolean isTokenMode);

  /**
   * Invalidates the internal cache containing the keys that are part of the CQ query results.
   * Once this method finishes, the CQ engine will not apply the internal optimization for already
   * seen keys anymore, not until the cache is manually rebuilt.
   */
  void invalidateCqResultKeys();

  /**
   * Sets the CQ Results key cache state as initialized.
   */
  void setCqResultsCacheInitialized();

  /**
   *
   * @return true if the CQ Results key cache is initialized.
   */
  boolean isCqResultsCacheInitialized();

  /**
   * Returns true if old value is required for query processing.
   */
  boolean isOldValueRequiredForQueryProcessing(Object key);

  /**
   * Closes the Query. On Client side, sends the cq close request to server. On Server side, takes
   * care of repository cleanup.
   *
   * @param sendRequestToServer true to send the request to server.
   */
  @Override
  void close(boolean sendRequestToServer) throws CqClosedException, CqException;


  boolean isPR();

  ClientProxyMembershipID getClientProxyId();

  @Override
  void stop() throws CqClosedException, CqException;
}
