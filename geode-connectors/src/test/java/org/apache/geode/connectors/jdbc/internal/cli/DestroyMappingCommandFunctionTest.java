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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.Collections;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.asyncqueue.internal.InternalAsyncEventQueue;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.JdbcLoader;
import org.apache.geode.connectors.jdbc.JdbcWriter;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class DestroyMappingCommandFunctionTest {

  private static final String regionName = "testRegion";

  private DestroyMappingFunction function;
  private FunctionContext<String> context;
  private ResultSender<Object> resultSender;
  private RegionMapping mapping;
  private JdbcConnectorService service;
  private InternalCache cache;
  private RegionAttributes<Object, Object> regionAttributes;
  private AttributesMutator<Object, Object> regionMutator;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    cache = mock(InternalCache.class);
    Region<Object, Object> region = mock(Region.class);
    when(region.getName()).thenReturn(regionName);
    regionAttributes = mock(RegionAttributes.class);
    regionMutator = mock(AttributesMutator.class);
    when(region.getAttributes()).thenReturn(regionAttributes);
    when(region.getAttributesMutator()).thenReturn(regionMutator);
    when(cache.getRegion(regionName)).thenReturn(region);
    context = mock(FunctionContext.class);
    when(context.getMemberName()).thenReturn("myMemberName");
    DistributedMember member = mock(DistributedMember.class);
    resultSender = mock(ResultSender.class);
    service = mock(JdbcConnectorService.class);
    DistributedSystem system = mock(DistributedSystem.class);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(member);
    when(context.getArguments()).thenReturn(regionName);
    when(cache.getService(eq(JdbcConnectorService.class))).thenReturn(service);

    mapping = new RegionMapping();

    function = new DestroyMappingFunction();
  }

  @Test
  public void isHAReturnsFalse() {
    assertThat(function.isHA()).isFalse();
  }

  @Test
  public void getIdReturnsNameOfClass() {
    assertThat(function.getId()).isEqualTo(function.getClass().getName());
  }

  @Test
  public void serializes() {
    Serializable original = function;

    Object copy = SerializationUtils.clone(original);

    assertThat(copy).isNotSameAs(original).isInstanceOf(DestroyMappingFunction.class);
  }

  @Test
  public void executeFunctionGivenExistingMappingReturnsTrue() {
    when(service.getMappingForRegion(eq(regionName))).thenReturn(mapping);

    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.toString())
        .contains("Destroyed JDBC mapping for region " + regionName + " on myMemberName");
  }

  @Test
  public void executeFunctionGivenNoExistingMappingReturnsFalse() {
    CliFunctionResult result = function.executeFunction(context);

    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.toString())
        .contains("JDBC mapping for region \"" + regionName + "\" not found");
  }

  @Test
  public void executeFunctionGivenARegionWithJdbcLoaderRemovesTheLoader() {
    @SuppressWarnings("unchecked")
    final JdbcLoader<Object, Object> mockJdbcLoader = mock(JdbcLoader.class);
    when(regionAttributes.getCacheLoader()).thenReturn(mockJdbcLoader);
    when(service.getMappingForRegion(eq(regionName))).thenReturn(mapping);

    function.executeFunction(context);

    verify(regionMutator, times(1)).setCacheLoader(null);
  }

  @Test
  public void executeFunctionGivenARegionWithNonJdbcLoaderDoesNotRemoveTheLoader() {
    @SuppressWarnings("unchecked")
    final CacheLoader<Object, Object> mockCacheLoader = mock(CacheLoader.class);
    when(regionAttributes.getCacheLoader()).thenReturn(mockCacheLoader);
    when(service.getMappingForRegion(eq(regionName))).thenReturn(mapping);

    function.executeFunction(context);

    verify(regionMutator, never()).setCacheLoader(null);
  }

  @Test
  public void executeFunctionGivenARegionWithJdbcWriterRemovesTheWriter() {
    @SuppressWarnings("unchecked")
    final JdbcWriter<Object, Object> mock = mock(JdbcWriter.class);
    when(regionAttributes.getCacheWriter()).thenReturn(mock);
    when(service.getMappingForRegion(eq(regionName))).thenReturn(mapping);

    function.executeFunction(context);

    verify(regionMutator, times(1)).setCacheWriter(null);
  }

  @Test
  public void executeFunctionGivenARegionWithNonJdbcWriterDoesNotRemoveTheWriter() {
    @SuppressWarnings("unchecked")
    final CacheWriter<Object, Object> mock = mock(CacheWriter.class);
    when(regionAttributes.getCacheWriter()).thenReturn(mock);
    when(service.getMappingForRegion(eq(regionName))).thenReturn(mapping);

    function.executeFunction(context);

    verify(regionMutator, never()).setCacheWriter(null);
  }

  @Test
  public void executeFunctionGivenARegionWithJdbcAsyncEventQueueRemovesTheQueueName() {
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    when(regionAttributes.getAsyncEventQueueIds()).thenReturn(Collections.singleton(queueName));
    when(service.getMappingForRegion(eq(regionName))).thenReturn(mapping);

    function.executeFunction(context);

    verify(regionMutator, times(1)).removeAsyncEventQueueId(queueName);
  }

  @Test
  public void executeFunctionGivenARegionWithNonJdbcAsyncEventQueueDoesNotRemoveTheQueueName() {
    when(regionAttributes.getAsyncEventQueueIds())
        .thenReturn(Collections.singleton("nonJdbcQueue"));
    when(service.getMappingForRegion(eq(regionName))).thenReturn(mapping);

    function.executeFunction(context);

    verify(regionMutator, never()).removeAsyncEventQueueId(any());
  }

  @Test
  public void executeFunctionGivenAJdbcAsyncWriterQueueRemovesTheQueue() {
    String queueName = MappingCommandUtils.createAsyncEventQueueName(regionName);
    InternalAsyncEventQueue myQueue = mock(InternalAsyncEventQueue.class);
    when(cache.getAsyncEventQueue(queueName)).thenReturn(myQueue);

    when(service.getMappingForRegion(eq(regionName))).thenReturn(mapping);

    function.executeFunction(context);

    verify(myQueue, times(1)).stop();
    verify(myQueue, times(1)).destroy();
  }

  @Test
  public void executeFunctionGivenExistingMappingCallsDestroyRegionMapping() {
    when(service.getMappingForRegion(eq(regionName))).thenReturn(mapping);

    function.executeFunction(context);

    verify(service, times(1)).destroyRegionMapping(eq(regionName));
  }

  @Test
  public void executeReportsErrorIfMappingNotFound() {
    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getStatusMessage())
        .contains("JDBC mapping for region \"" + regionName + "\" not found");
  }
}
