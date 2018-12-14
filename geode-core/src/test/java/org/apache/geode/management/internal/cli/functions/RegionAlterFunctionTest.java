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
package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.EvictionAttributesMutator;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;

public class RegionAlterFunctionTest {

  private RegionAlterFunction function;
  private RegionConfig config;
  private RegionAttributesType regionAttributes;
  private InternalCache internalCache;
  private InternalCacheForClientAccess cache;
  private FunctionContext<RegionConfig> context;
  private AttributesMutator mutator;
  private EvictionAttributesMutator evictionMutator;
  private AbstractRegion region;

  @Before
  public void setUp() throws Exception {
    function = spy(RegionAlterFunction.class);
    config = new RegionConfig();
    regionAttributes = new RegionAttributesType();
    config.setRegionAttributes(regionAttributes);

    internalCache = mock(InternalCache.class);
    cache = mock(InternalCacheForClientAccess.class);
    mutator = mock(AttributesMutator.class);
    evictionMutator = mock(EvictionAttributesMutator.class);
    when(mutator.getEvictionAttributesMutator()).thenReturn(evictionMutator);
    region = mock(AbstractRegion.class);

    context = mock(FunctionContext.class);
    when(context.getCache()).thenReturn(internalCache);
    when(internalCache.getCacheForProcessingClientRequests()).thenReturn(cache);
    when(context.getArguments()).thenReturn(config);
    when(context.getMemberName()).thenReturn("member");
    when(cache.getRegion(any())).thenReturn(region);
    when(region.getAttributesMutator()).thenReturn(mutator);
  }

  @Test
  public void executeFuntcionHappyPathRetunsStatusOK() {
    doNothing().when(function).alterRegion(any(), any());
    config.setName("regionA");
    CliFunctionResult result = function.executeFunction(context);
    assertThat(result.getMemberIdOrName()).isEqualTo("member");
    assertThat(result.getStatus()).isEqualTo("OK");
    assertThat(result.getStatusMessage()).isEqualTo("Region regionA altered");
  }

  @Test
  public void alterRegionWithNullRegionThrowsIllegalArgumentException() {
    when(cache.getRegion(anyString())).thenReturn(null);
    config.setName("regionA");
    assertThatThrownBy(() -> function.alterRegion(cache, config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Region does not exist: regionA");
  }

  @Test
  public void updateWithEmptyRegionAttributes() {
    // the regionAttributes starts with no values inside
    function.alterRegion(cache, config);
    verifyZeroInteractions(mutator);
  }

  @Test
  public void updateWithCloningEnabled() throws Exception {
    regionAttributes.setCloningEnabled(false);
    function.alterRegion(cache, config);
    verify(mutator).setCloningEnabled(false);
  }

  @Test
  public void updateWithEvictionAttributes() throws Exception {
    RegionAttributesType.EvictionAttributes evictionAttributes =
        new RegionAttributesType.EvictionAttributes();
    RegionAttributesType.EvictionAttributes.LruEntryCount lruEntryCount =
        new RegionAttributesType.EvictionAttributes.LruEntryCount();
    lruEntryCount.setMaximum("10");
    evictionAttributes.setLruEntryCount(lruEntryCount);
    regionAttributes.setEvictionAttributes(evictionAttributes);

    function.alterRegion(cache, config);
    verify(mutator).getEvictionAttributesMutator();
    verify(evictionMutator).setMaximum(10);
  }

  @Test
  public void updateWithEntryIdleTime_timeoutAndAction() throws Exception {
    RegionAttributesType.ExpirationAttributesType expiration =
        new RegionAttributesType.ExpirationAttributesType();
    regionAttributes.setEntryIdleTime(expiration);
    expiration.setTimeout("10");
    expiration.setAction("invalidate");

    ExpirationAttributes existing = new ExpirationAttributes();
    when(region.getEntryIdleTimeout()).thenReturn(existing);

    function.alterRegion(cache, config);

    verify(mutator).setEntryIdleTimeout(existing);
    assertThat(existing.getTimeout()).isEqualTo(10);
    assertThat(existing.getAction()).isEqualTo(ExpirationAction.INVALIDATE);
    verify(mutator, times(0)).setCustomEntryIdleTimeout(any());
  }

  @Test
  public void updateWithEntryIdleTime_TimeoutOnly() throws Exception {
    RegionAttributesType.ExpirationAttributesType expiration =
        new RegionAttributesType.ExpirationAttributesType();
    regionAttributes.setEntryIdleTime(expiration);
    expiration.setTimeout("10");

    ExpirationAttributes existing = new ExpirationAttributes();
    existing.setTimeout(20);
    existing.setAction(ExpirationAction.DESTROY);
    when(region.getEntryIdleTimeout()).thenReturn(existing);

    function.alterRegion(cache, config);

    verify(mutator).setEntryIdleTimeout(existing);
    assertThat(existing.getTimeout()).isEqualTo(10);
    assertThat(existing.getAction()).isEqualTo(ExpirationAction.DESTROY);
    verify(mutator, times(0)).setCustomEntryIdleTimeout(any());
  }

  @Test
  public void updateWithCustomExpiry() throws Exception {
    RegionAttributesType.ExpirationAttributesType expiration =
        new RegionAttributesType.ExpirationAttributesType();
    regionAttributes.setEntryIdleTime(expiration);
    DeclarableType mockExpiry = mock(DeclarableType.class);
    when(mockExpiry.newInstance(any())).thenReturn(mock(CustomExpiry.class));
    expiration.setCustomExpiry(mockExpiry);

    function.alterRegion(cache, config);

    verify(mutator, times(0)).setEntryIdleTimeout(any());
    verify(mutator).setCustomEntryIdleTimeout(notNull());
  }

  @Test
  public void deleteCustomExpiry() throws Exception {
    RegionAttributesType.ExpirationAttributesType expiration =
        new RegionAttributesType.ExpirationAttributesType();
    regionAttributes.setEntryIdleTime(expiration);
    expiration.setCustomExpiry(DeclarableType.EMPTY);

    function.alterRegion(cache, config);

    verify(mutator, times(0)).setEntryIdleTimeout(any());
    verify(mutator).setCustomEntryIdleTimeout(null);
  }

  @Test
  public void updateWithGatewaySenders() throws Exception {
    regionAttributes.setGatewaySenderIds("2,3");
    when(region.getGatewaySenderIds()).thenReturn(new HashSet<>(Arrays.asList("1", "2")));

    function.alterRegion(cache, config);

    verify(mutator).removeGatewaySenderId("1");
    verify(mutator, times(0)).removeGatewaySenderId("2");
    verify(mutator).addGatewaySenderId("3");

    // asyncEventQueue is left intact
    verify(mutator, times(0)).addAsyncEventQueueId(any());
    verify(mutator, times(0)).removeAsyncEventQueueId(any());
  }

  @Test
  public void updateWithEmptyGatewaySenders() throws Exception {
    regionAttributes.setGatewaySenderIds("");
    when(region.getGatewaySenderIds()).thenReturn(new HashSet<>(Arrays.asList("1", "2")));

    function.alterRegion(cache, config);

    verify(mutator).removeGatewaySenderId("1");
    verify(mutator).removeGatewaySenderId("2");
  }

  @Test
  public void updateWithCacheListeners() throws Exception {
    // suppose region has one cacheListener, and we want to replace the oldOne one with the new one
    CacheListener oldOne = mock(CacheListener.class);
    CacheListener newOne = mock(CacheListener.class);
    when(region.getCacheListeners()).thenReturn(new CacheListener[] {oldOne});

    DeclarableType newCacheListenerType = mock(DeclarableType.class);
    when(newCacheListenerType.newInstance(any())).thenReturn(newOne);
    regionAttributes.getCacheListeners().add(newCacheListenerType);

    function.alterRegion(cache, config);
    verify(mutator).removeCacheListener(oldOne);
    verify(mutator).addCacheListener(newOne);
  }

  @Test
  public void updateWithEmptyCacheListeners() throws Exception {
    // suppose region has on listener, and we want to delete that one
    CacheListener oldOne = mock(CacheListener.class);
    when(region.getCacheListeners()).thenReturn(new CacheListener[] {oldOne});
    regionAttributes.getCacheListeners().add(DeclarableType.EMPTY);

    function.alterRegion(cache, config);
    verify(mutator).removeCacheListener(oldOne);
    verify(mutator, times(0)).addCacheListener(any());
  }

  @Test
  public void updateWithCacheWriter() throws Exception {
    DeclarableType newCacheWriterDeclarable = mock(DeclarableType.class);
    when(newCacheWriterDeclarable.newInstance(any())).thenReturn(mock(CacheWriter.class));
    regionAttributes.setCacheWriter(newCacheWriterDeclarable);

    function.alterRegion(cache, config);
    verify(mutator).setCacheWriter(notNull());
    verify(mutator, times(0)).setCacheLoader(any());
  }

  @Test
  public void updateWithNoCacheWriter() throws Exception {
    regionAttributes.setCacheWriter(DeclarableType.EMPTY);

    function.alterRegion(cache, config);
    verify(mutator).setCacheWriter(null);
    verify(mutator, times(0)).setCacheLoader(any());
  }
}
