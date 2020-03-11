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
package org.apache.geode.cache.client.internal;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;

public class InvalidateOpJUnitTest {
  private LocalRegion region;
  private EntryEventImpl event;
  private PoolImpl pool;
  private KeyInfo keyinfo;
  private InternalCache cache;
  private ClientMetadataService cms;
  private EventID eventID;

  @Before
  public void setup() {
    region = mock(LocalRegion.class);
    event = mock(EntryEventImpl.class);
    pool = mock(PoolImpl.class);
    keyinfo = mock(KeyInfo.class);
    cache = mock(InternalCache.class);
    cms = mock(ClientMetadataService.class);
    eventID = new EventID();

    when(event.getCallbackArgument()).thenReturn(null);
    when(event.getKeyInfo()).thenReturn(keyinfo);
    when(keyinfo.getKey()).thenReturn("key");
    when(event.getEventId()).thenReturn(eventID);
    when(event.getKey()).thenReturn("key");
  }

  @Test
  public void testInvalidateOperation() {

    InvalidateOp.execute(pool, "testregion", event, false, region);
    verify(pool).execute(any());
  }

  @Test
  public void testInvalidateOpSingleHopNoServer() {
    when(event.getKey()).thenReturn("key");
    when(region.getCache()).thenReturn(cache);
    when(cache.getClientMetadataService()).thenReturn(cms);
    when(cms.getBucketServerLocation(region, Operation.INVALIDATE, "key", null,
        null)).thenReturn(null);

    InvalidateOp.execute(pool, "testregion", event, true, region);
    verify(pool).execute(any());
  }

  @Test
  public void testInvalidateOpSingleHop() {
    ServerLocation server = mock(ServerLocation.class);

    when(region.getCache()).thenReturn(cache);
    when(cache.getClientMetadataService()).thenReturn(cms);
    when(cms.getBucketServerLocation(region, Operation.INVALIDATE, "key", null,
        null)).thenReturn(server);
    when(pool.getMaxConnections()).thenReturn(10);
    when(pool.getConnectionCount()).thenReturn(2);

    InvalidateOp.execute(pool, "testregion", event, true, region);
    verify(pool).executeOn((ServerLocation) any(), any(), eq(true), eq(false));
  }

  @Test
  public void testInvalidateOpSingleHopUseExistConnect() {
    ServerLocation server = mock(ServerLocation.class);

    when(region.getCache()).thenReturn(cache);
    when(cache.getClientMetadataService()).thenReturn(cms);
    when(cms.getBucketServerLocation(region, Operation.INVALIDATE, "key", null,
        null)).thenReturn(server);
    when(pool.getMaxConnections()).thenReturn(10);
    when(pool.getConnectionCount()).thenReturn(10);

    InvalidateOp.execute(pool, "testregion", event, true, region);
    verify(pool).executeOn((ServerLocation) any(), any(), eq(true), eq(true));
  }
}
