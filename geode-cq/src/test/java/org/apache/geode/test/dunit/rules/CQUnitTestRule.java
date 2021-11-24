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

package org.apache.geode.test.dunit.rules;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.rules.ExternalResource;

import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ChunkedMessage;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

public class CQUnitTestRule extends ExternalResource {
  public SecurityService securityService;
  public Message message;
  public ServerConnection connection;
  public InternalCache cache;
  public CqService cqService;
  public InternalCqQuery internalCqQuery;

  @Override
  protected void before() throws Throwable {
    securityService = mock(SecurityService.class);
    message = mock(Message.class);
    connection = mock(ServerConnection.class);
    cache = mock(InternalCache.class);
    cqService = mock(CqService.class);
    internalCqQuery = mock(InternalCqQuery.class);
    String regionName = "regionName";
    Part part = mock(Part.class);
    CachedRegionHelper crHelper = mock(CachedRegionHelper.class);

    DefaultQueryService queryService = mock(DefaultQueryService.class);
    DefaultQuery query = mock(DefaultQuery.class);

    Set<String> regionsInQuery = new HashSet<>();
    regionsInQuery.add(regionName);

    when(connection.getCachedRegionHelper()).thenReturn(crHelper);
    when(connection.getCacheServerStats()).thenReturn(mock(CacheServerStats.class));
    when(connection.getAcceptor()).thenReturn(mock(AcceptorImpl.class));
    when(connection.getChunkedResponseMessage()).thenReturn(mock(ChunkedMessage.class));
    when(message.getPart(anyInt())).thenReturn(part);
    when(part.getString()).thenReturn("CQ");
    when(part.getInt()).thenReturn(10);
    when(part.getSerializedForm()).thenReturn(new byte[] {0, 0});
    when(crHelper.getCache()).thenReturn(cache);
    when(cache.getCqService()).thenReturn(cqService);
    when(cache.getLocalQueryService()).thenReturn(queryService);
    when(queryService.newQuery(anyString())).thenReturn(query);
    when(query.getRegionsInQuery(null)).thenReturn(regionsInQuery);
    when(cqService.getCq("CQ")).thenReturn(internalCqQuery);
    when(internalCqQuery.getRegionName()).thenReturn(regionName);
  }

}
