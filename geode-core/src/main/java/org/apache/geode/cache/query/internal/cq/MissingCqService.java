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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.DataPolicy;
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

public class MissingCqService implements CqService {

  @Override
  public ClientCQ newCq(String cqName, String queryString, CqAttributes cqAttributes,
      InternalPool serverProxy, boolean isDurable)
      throws QueryInvalidException, CqExistsException, CqException {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public CqQuery getClientCqFromServer(ClientProxyMembershipID clientProxyId, String clientCqName) {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public InternalCqQuery getCq(String cqName) {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public Collection<? extends InternalCqQuery> getAllCqs() {
    return Collections.emptyList();
  }

  @Override
  public Collection<? extends InternalCqQuery> getAllCqs(String regionName) throws CqException {
    return Collections.emptyList();
  }

  @Override
  public void executeAllClientCqs() {}

  @Override
  public void executeAllRegionCqs(String regionName) {}

  @Override
  public void executeCqs(Collection<? extends InternalCqQuery> cqs) throws CqException {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public void stopAllClientCqs() {}

  @Override
  public void stopAllRegionCqs(String regionName) {}

  @Override
  public void stopCqs(Collection<? extends InternalCqQuery> cqs) throws CqException {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public void closeCqs(String regionName) throws CqException {}

  @Override
  public void closeCq(String cqName, ClientProxyMembershipID clientProxyId) throws CqException {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public void closeAllCqs(boolean clientInitiated) {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public void closeAllCqs(boolean clientInitiated, Collection<? extends InternalCqQuery> cqs,
      boolean keepAlive) {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public CqServiceStatistics getCqStatistics() {
    return new MissingCqServiceStatistics();
  }

  @Override
  public void closeClientCqs(ClientProxyMembershipID clientProxyId) throws CqException {}

  @Override
  public List<ServerCQ> getAllClientCqs(ClientProxyMembershipID clientProxyId) {
    return Collections.emptyList();
  }

  @Override
  public List<String> getAllDurableClientCqs(ClientProxyMembershipID clientProxyId) {
    return Collections.emptyList();
  }

  @Override
  public void dispatchCqListeners(HashMap<String, Integer> cqs, int messageType, Object key,
      Object value,
      byte[] delta, QueueManager qManager, EventID eventId) {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public void processEvents(CacheEvent<?, ?> event, Profile localProfile, Profile[] profiles,
      FilterRoutingInfo frInfo) throws CqException {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public UserAttributes getUserAttributes(String cqName) {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public void close() {}

  @Override
  public boolean isRunning() {
    return false;
  }

  @Override
  public void start() {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public String constructServerCqName(String cqName, ClientProxyMembershipID clientProxyId) {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public void stopCq(String cqName, ClientProxyMembershipID clientId) throws CqException {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public void cqsDisconnected(Pool pool) {}

  @Override
  public void cqsConnected(Pool pool) {}

  @Override
  public ServerCQ executeCq(@NotNull String cqName, @NotNull String queryString,
      int cqState,
      @NotNull ClientProxyMembershipID clientProxyId,
      @Nullable CacheClientNotifier ccn, boolean isDurable,
      boolean manageEmptyRegions,
      @Nullable DataPolicy regionDataPolicy,
      @NotNull Map<String, Integer> emptyRegionsMap)
      throws CqException, RegionNotFoundException, CqClosedException {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public void closeNonDurableClientCqs(ClientProxyMembershipID clientProxyId) throws CqException {}

  @Override
  public List<String> getAllDurableCqsFromServer(InternalPool pool) {
    throw new IllegalStateException("CqService is not available.");
  }

  @Override
  public void resumeCQ(int cqState, ServerCQ cQuery) {
    throw new IllegalStateException("CqService is not available.");
  }
}
