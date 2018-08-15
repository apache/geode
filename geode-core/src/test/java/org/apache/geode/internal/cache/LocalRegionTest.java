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


import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;


public class LocalRegionTest {

  private final Object key = new Object();

  @Test
  public void proxyRegionNeedsPendingCallbacksForDestroy() {
    TXEntryState txEntryState = mock(TXEntryState.class);
    LocalRegion region = mock(LocalRegion.class);
    when(region.isProxy()).thenReturn(true);
    when(region.needsPendingCallbacksForDestroy(key, txEntryState)).thenCallRealMethod();

    assertThat(region.needsPendingCallbacksForDestroy(key, txEntryState)).isTrue();
  }

  @Test
  public void destroyNullOriginalValueOnTransactionHostDoesNotNeedPendingCallbacksForDestroy() {
    TXEntryState txEntryState = mock(TXEntryState.class);
    LocalRegion region = mock(LocalRegion.class);
    when(region.isProxy()).thenReturn(false);
    when(txEntryState.getOriginalValue()).thenReturn(null);
    when(region.needsPendingCallbacksForDestroy(key, txEntryState)).thenCallRealMethod();

    assertThat(region.needsPendingCallbacksForDestroy(key, txEntryState)).isFalse();
  }

  @Test
  public void destroyDestroyedTokenOnTransactionHostDoesNotNeedPendingCallbacksForDestroy() {
    TXEntryState txEntryState = mock(TXEntryState.class);
    LocalRegion region = mock(LocalRegion.class);
    when(region.isProxy()).thenReturn(false);
    when(txEntryState.getOriginalValue()).thenReturn(Token.DESTROYED);
    when(region.needsPendingCallbacksForDestroy(key, txEntryState)).thenCallRealMethod();

    assertThat(region.needsPendingCallbacksForDestroy(key, txEntryState)).isFalse();
  }

  @Test
  public void destroyTombstoneOnTransactionHostDoesNotNeedPendingCallbacksForDestroy() {
    TXEntryState txEntryState = mock(TXEntryState.class);
    LocalRegion region = mock(LocalRegion.class);
    when(region.isProxy()).thenReturn(false);
    when(txEntryState.getOriginalValue()).thenReturn(Token.TOMBSTONE);
    when(region.needsPendingCallbacksForDestroy(key, txEntryState)).thenCallRealMethod();

    assertThat(region.needsPendingCallbacksForDestroy(key, txEntryState)).isFalse();
  }

  @Test
  public void destroyNotATokenOnTransactionHostNeedsPendingCallbacksForDestroy() {
    TXEntryState txEntryState = mock(TXEntryState.class);
    LocalRegion region = mock(LocalRegion.class);
    when(region.isProxy()).thenReturn(false);
    when(txEntryState.getOriginalValue()).thenReturn(new Token.NotAToken());
    when(region.needsPendingCallbacksForDestroy(key, txEntryState)).thenCallRealMethod();

    assertThat(region.needsPendingCallbacksForDestroy(key, txEntryState)).isTrue();
  }

  @Test
  public void destroyNonExistingRegionEntryOnRemoteHostDoesNotNeedPendingCallbacksForDestroy() {
    LocalRegion region = mock(LocalRegion.class);
    when(region.basicGetEntry(key)).thenReturn(null);
    when(region.isProxy()).thenReturn(false);
    when(region.needsPendingCallbacksForDestroy(key, null)).thenCallRealMethod();

    assertThat(region.needsPendingCallbacksForDestroy(key, null)).isFalse();
  }

  @Test
  public void destroyRemovedTokenOnRemoteHostDoesNotNeedPendingCallbacksForDestroy() {
    LocalRegion region = mock(LocalRegion.class);
    RegionEntry regionEntry = mock(RegionEntry.class);
    when(region.isProxy()).thenReturn(false);
    when(region.isProxy()).thenReturn(false);
    when(region.basicGetEntry(key)).thenReturn(regionEntry);
    when(regionEntry.isDestroyedOrRemoved()).thenReturn(true);
    when(region.needsPendingCallbacksForDestroy(key, null)).thenCallRealMethod();

    assertThat(region.needsPendingCallbacksForDestroy(key, null)).isFalse();
  }

  @Test
  public void destroyAValueOnRemoteHostNeedsPendingCallbacksForDestroy() {
    LocalRegion region = mock(LocalRegion.class);
    RegionEntry regionEntry = mock(RegionEntry.class);
    when(region.isProxy()).thenReturn(false);
    when(region.isProxy()).thenReturn(false);
    when(region.basicGetEntry(key)).thenReturn(regionEntry);
    when(regionEntry.isDestroyedOrRemoved()).thenReturn(false);
    when(region.needsPendingCallbacksForDestroy(key, null)).thenCallRealMethod();

    assertThat(region.needsPendingCallbacksForDestroy(key, null)).isTrue();
  }
}
