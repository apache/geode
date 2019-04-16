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

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.internal.ServerRegionProxy;

public class LocalRegionTest {
  private LocalRegion region;
  private EntryEventImpl event;
  private ServerRegionProxy serverRegionProxy;
  private Operation operation;
  private CancelCriterion cancelCriterion;
  private RegionAttributes regionAttributes;

  private final Object key = new Object();
  private final String value = "value";

  @Before
  public void setup() {
    region = mock(LocalRegion.class);
    event = mock(EntryEventImpl.class);
    serverRegionProxy = mock(ServerRegionProxy.class);
    cancelCriterion = mock(CancelCriterion.class);
    regionAttributes = mock(RegionAttributes.class);

    when(region.getServerProxy()).thenReturn(serverRegionProxy);
    when(event.isFromServer()).thenReturn(false);
    when(event.getKey()).thenReturn(key);
    when(event.getRawNewValue()).thenReturn(value);
    when(region.getCancelCriterion()).thenReturn(cancelCriterion);
    when(region.getAttributes()).thenReturn(regionAttributes);
  }

  @Test
  public void serverPutWillCheckPutIfAbsentResult() {
    Object result = new Object();
    operation = Operation.PUT_IF_ABSENT;
    when(event.getOperation()).thenReturn(operation);
    when(event.isCreate()).thenReturn(true);
    when(serverRegionProxy.put(key, value, null, event, operation, true, null, null, true))
        .thenReturn(result);
    doCallRealMethod().when(region).serverPut(event, true, null);

    region.serverPut(event, true, null);

    verify(region).checkPutIfAbsentResult(event, value, result);
  }

  @Test
  public void checkPutIfAbsentResultSucceedsIfResultIsNull() {
    Object result = null;
    doCallRealMethod().when(region).checkPutIfAbsentResult(event, value, result);

    region.checkPutIfAbsentResult(event, value, result);

    verify(event, never()).hasRetried();
  }

  @Test(expected = EntryNotFoundException.class)
  public void checkPutIfAbsentResultThrowsIfResultNotNullAndEventHasNotRetried() {
    Object result = new Object();
    when(event.hasRetried()).thenReturn(false);
    doCallRealMethod().when(region).checkPutIfAbsentResult(event, value, result);

    region.checkPutIfAbsentResult(event, value, result);
  }

}
