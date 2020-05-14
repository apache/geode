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
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EvictionAttributesMutator;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.CacheWriterAdapter;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class RegionAlterFunctionTest {
  private RegionAlterFunction function;
  private RegionConfig config;
  private RegionAttributesType regionAttributes;
  private InternalCacheForClientAccess cache;
  private FunctionContext<RegionConfig> context;
  private AttributesMutator<Object, Object> mutator;
  private EvictionAttributesMutator evictionMutator;
  private AbstractRegion region;

  public static class MyCustomExpiry implements CustomExpiry<Object, Object>, Declarable {
    @Override
    public ExpirationAttributes getExpiry(Region.Entry<Object, Object> entry) {
      return null;
    }
  }

  public static class MyCacheListener extends CacheListenerAdapter<Object, Object> {
  }

  public static class MyCacheWriter extends CacheWriterAdapter<Object, Object> {
  }

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    function = spy(RegionAlterFunction.class);
    config = new RegionConfig();
    regionAttributes = new RegionAttributesType();
    config.setRegionAttributes(regionAttributes);

    InternalCache internalCache = mock(InternalCache.class);
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
  public void executeFunctionHappyPathReturnsStatusOK() {
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
  public void updateWithCloningEnabled() {
    regionAttributes.setCloningEnabled(false);
    function.alterRegion(cache, config);
    verify(mutator).setCloningEnabled(false);
  }

  @Test
  public void updateWithEvictionAttributes() {
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
  public void updateWithEntryIdleTime_timeoutAndAction() {
    RegionAttributesType.ExpirationAttributesType expiration =
        new RegionAttributesType.ExpirationAttributesType();
    regionAttributes.setEntryIdleTime(expiration);
    expiration.setTimeout("10");
    expiration.setAction("invalidate");

    ExpirationAttributes existing = new ExpirationAttributes();
    when(region.getEntryIdleTimeout()).thenReturn(existing);

    function.alterRegion(cache, config);

    ArgumentCaptor<ExpirationAttributes> updatedCaptor =
        ArgumentCaptor.forClass(ExpirationAttributes.class);
    verify(mutator).setEntryIdleTimeout(updatedCaptor.capture());
    assertThat(updatedCaptor.getValue().getTimeout()).isEqualTo(10);
    assertThat(updatedCaptor.getValue().getAction()).isEqualTo(ExpirationAction.INVALIDATE);
    verify(mutator, times(0)).setCustomEntryIdleTimeout(any());
  }

  @Test
  public void updateWithEntryIdleTime_TimeoutOnly() {
    RegionAttributesType.ExpirationAttributesType expiration =
        new RegionAttributesType.ExpirationAttributesType();
    regionAttributes.setEntryIdleTime(expiration);
    expiration.setTimeout("10");

    ExpirationAttributes existing = new ExpirationAttributes(20, ExpirationAction.DESTROY);
    when(region.getEntryIdleTimeout()).thenReturn(existing);

    function.alterRegion(cache, config);

    ArgumentCaptor<ExpirationAttributes> updatedCaptor =
        ArgumentCaptor.forClass(ExpirationAttributes.class);
    verify(mutator).setEntryIdleTimeout(updatedCaptor.capture());
    assertThat(updatedCaptor.getValue().getTimeout()).isEqualTo(10);
    assertThat(updatedCaptor.getValue().getAction()).isEqualTo(ExpirationAction.DESTROY);
    verify(mutator, times(0)).setCustomEntryIdleTimeout(any());
  }

  @Test
  public void updateWithCustomExpiry() {
    RegionAttributesType.ExpirationAttributesType expiration =
        new RegionAttributesType.ExpirationAttributesType();
    regionAttributes.setEntryIdleTime(expiration);
    DeclarableType mockExpiry = mock(DeclarableType.class);
    when(mockExpiry.getClassName()).thenReturn(MyCustomExpiry.class.getName());
    expiration.setCustomExpiry(mockExpiry);

    function.alterRegion(cache, config);

    verify(mutator, times(0)).setEntryIdleTimeout(any());
    verify(mutator).setCustomEntryIdleTimeout(notNull());
  }

  @Test
  public void deleteCustomExpiry() {
    RegionAttributesType.ExpirationAttributesType expiration =
        new RegionAttributesType.ExpirationAttributesType();
    regionAttributes.setEntryIdleTime(expiration);
    expiration.setCustomExpiry(DeclarableType.EMPTY);

    function.alterRegion(cache, config);

    verify(mutator, times(0)).setEntryIdleTimeout(any());
    verify(mutator).setCustomEntryIdleTimeout(null);
  }

  @Test
  public void updateWithGatewaySenders() {
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
  public void updateWithEmptyGatewaySenders() {
    regionAttributes.setGatewaySenderIds("");
    when(region.getGatewaySenderIds()).thenReturn(new HashSet<>(Arrays.asList("1", "2")));

    function.alterRegion(cache, config);

    verify(mutator).removeGatewaySenderId("1");
    verify(mutator).removeGatewaySenderId("2");
  }

  @Test
  public void updateWithAsynchronousEventQueues() {
    regionAttributes.setAsyncEventQueueIds("queue2,queue3");
    when(region.getAsyncEventQueueIds())
        .thenReturn(new HashSet<>(Arrays.asList("queue1", "queue2")));
    function.alterRegion(cache, config);

    verify(mutator).removeAsyncEventQueueId("queue1");
    verify(mutator, times(0)).removeAsyncEventQueueId("queue2");
    verify(mutator).addAsyncEventQueueId("queue3");

    // gatewaySender is left intact
    verify(mutator, times(0)).addGatewaySenderId(any());
    verify(mutator, times(0)).removeGatewaySenderId(any());
  }

  @Test
  public void updateWithEmptyAsynchronousEventQueues() {
    regionAttributes.setAsyncEventQueueIds("");
    when(region.getAsyncEventQueueIds())
        .thenReturn(new HashSet<>(Arrays.asList("queue1", "queue2")));
    function.alterRegion(cache, config);

    verify(mutator).removeAsyncEventQueueId("queue1");
    verify(mutator).removeAsyncEventQueueId("queue2");
  }

  @Test
  public void updateWithCacheListeners() {
    // suppose region has one cacheListener, and we want to replace the oldOne one with the new one
    @SuppressWarnings("unchecked")
    CacheListener<Object, Object> oldOne = mock(CacheListener.class);
    when(region.getCacheListeners()).thenReturn(new CacheListener[] {oldOne});

    DeclarableType newCacheListenerType = mock(DeclarableType.class);
    when(newCacheListenerType.getClassName()).thenReturn(MyCacheListener.class.getName());
    regionAttributes.getCacheListeners().add(newCacheListenerType);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<CacheListener<Object, Object>> argument =
        ArgumentCaptor.forClass(CacheListener.class);

    function.alterRegion(cache, config);
    verify(mutator).removeCacheListener(oldOne);
    verify(mutator).addCacheListener(argument.capture());
    assertThat(argument.getValue()).isInstanceOf(MyCacheListener.class);
  }

  @Test
  public void updateWithEmptyCacheListeners() {
    // suppose region has on listener, and we want to delete that one
    @SuppressWarnings("unchecked")
    CacheListener<Object, Object> oldOne = mock(CacheListener.class);
    when(region.getCacheListeners()).thenReturn(new CacheListener[] {oldOne});
    regionAttributes.getCacheListeners().add(DeclarableType.EMPTY);

    function.alterRegion(cache, config);
    verify(mutator).removeCacheListener(oldOne);
    verify(mutator, times(0)).addCacheListener(any());
  }

  @Test
  public void updateWithCacheWriter() {
    DeclarableType newCacheWriterDeclarable = mock(DeclarableType.class);
    when(newCacheWriterDeclarable.getClassName()).thenReturn(MyCacheWriter.class.getName());
    regionAttributes.setCacheWriter(newCacheWriterDeclarable);

    function.alterRegion(cache, config);
    verify(mutator).setCacheWriter(notNull());
    verify(mutator, times(0)).setCacheLoader(any());
  }

  @Test
  public void updateWithNoCacheWriter() {
    regionAttributes.setCacheWriter(DeclarableType.EMPTY);

    function.alterRegion(cache, config);
    verify(mutator).setCacheWriter(null);
    verify(mutator, times(0)).setCacheLoader(any());
  }
}
