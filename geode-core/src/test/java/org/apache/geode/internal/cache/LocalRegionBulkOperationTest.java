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
package org.apache.geode.internal.cache;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.internal.ServerRegionProxy;

public class LocalRegionBulkOperationTest {
  private LocalRegion region;
  private EntryEventImpl event;
  private EventID eventID;
  private ServerRegionProxy serverRegionProxy;
  private CancelCriterion cancelCriterion;
  private PutAllPartialResultException exception;
  private final Object callbacks = new Object();
  private final CacheClosedException cacheClosedException = new CacheClosedException();

  @Before
  public void setup() {
    region = mock(LocalRegion.class);
    event = mock(EntryEventImpl.class);
    eventID = mock(EventID.class);
    serverRegionProxy = mock(ServerRegionProxy.class);
    cancelCriterion = mock(CancelCriterion.class);
    exception = mock(PutAllPartialResultException.class);

    when(event.getEventId()).thenReturn(eventID);
    when(event.getCallbackArgument()).thenReturn(callbacks);
    when(region.hasServerProxy()).thenReturn(true);
    when(region.getServerProxy()).thenReturn(serverRegionProxy);
    when(region.getRegionMap()).thenReturn(mock(RegionMap.class));
    when(region.getDataView()).thenReturn(mock(InternalDataView.class));
    when(region.getCancelCriterion()).thenReturn(cancelCriterion);
    when(exception.getFailure()).thenReturn(cacheClosedException);
    when(cancelCriterion.generateCancelledException(cacheClosedException))
        .thenReturn(cacheClosedException);
  }

  @Test(expected = CacheClosedException.class)
  public void basicRemoveAllThrowsCacheClosedExceptionIfCacheIsClosing() {
    String[] strings = {"key"};
    Set keys = new HashSet<>(Arrays.asList(strings));
    DistributedRemoveAllOperation removeAll = mock(DistributedRemoveAllOperation.class);
    when(removeAll.getBaseEvent()).thenReturn(event);
    when(region.basicRemoveAll(keys, removeAll, null)).thenCallRealMethod();
    when(serverRegionProxy.removeAll(keys, eventID, callbacks)).thenThrow(exception);

    region.basicRemoveAll(keys, removeAll, null);
  }

  @Test(expected = CacheClosedException.class)
  public void basicPutAllThrowsCacheClosedExceptionIfCacheIsClosing() {
    Map map = new HashMap();
    map.put("key", "value");
    DistributedPutAllOperation putAll = mock(DistributedPutAllOperation.class);
    when(putAll.getBaseEvent()).thenReturn(event);
    when(region.basicPutAll(map, putAll, null)).thenCallRealMethod();
    when(region.getAtomicThresholdInfo()).thenReturn(mock(MemoryThresholdInfo.class));
    when(serverRegionProxy.putAll(map, eventID, true, callbacks)).thenThrow(exception);

    region.basicPutAll(map, putAll, null);
  }
}
