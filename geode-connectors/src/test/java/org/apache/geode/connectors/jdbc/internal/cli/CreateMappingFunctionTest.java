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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class CreateMappingFunctionTest {

  private static final String REGION_NAME = "testRegion";

  private RegionMapping regionMapping;
  private FunctionContext<Object[]> context;
  private ResultSender<Object> resultSender;
  private JdbcConnectorService service;
  private InternalCache cache;
  private Region<Object, Object> region;
  private RegionAttributes<Object, Object> attributes;
  private AsyncEventQueueFactory asyncEventQueueFactory;
  private final Object[] functionInputs = new Object[2];

  private CreateMappingFunction function;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    cache = mock(InternalCache.class);
    region = mock(LocalRegion.class);
    DistributedSystem system = mock(DistributedSystem.class);
    DistributedMember distributedMember = mock(DistributedMember.class);
    service = mock(JdbcConnectorService.class);

    regionMapping = new RegionMapping(REGION_NAME, null, null, null, null, null, null);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(cache.getRegion(REGION_NAME)).thenReturn(region);
    when(system.getDistributedMember()).thenReturn(distributedMember);
    functionInputs[0] = regionMapping;
    setupAsynchronous();
    when(context.getArguments()).thenReturn(functionInputs);
    when(cache.getService(eq(JdbcConnectorService.class))).thenReturn(service);
    attributes = mock(RegionAttributes.class);
    when(attributes.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    when(region.getAttributes()).thenReturn(attributes);
    when(region.getAttributesMutator()).thenReturn(mock(AttributesMutator.class));
    asyncEventQueueFactory = mock(AsyncEventQueueFactory.class);
    when(cache.createAsyncEventQueueFactory()).thenReturn(asyncEventQueueFactory);

    function = new CreateMappingFunction();
  }

  private void setupAsynchronous() {
    functionInputs[1] = false;
  }

  private void setupSynchronous() {
    functionInputs[1] = true;
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

    assertThat(copy).isNotSameAs(original).isInstanceOf(CreateMappingFunction.class);
  }

  @Test
  public void createRegionMappingReturnsRegionName() throws Exception {
    function.createRegionMapping(service, regionMapping);

    verify(service, times(1)).createRegionMapping(regionMapping);
  }

  @Test
  public void createRegionMappingThrowsIfMappingExists() throws Exception {
    doAnswer((m) -> {
      throw new RegionMappingExistsException();
    }).when(service)
        .createRegionMapping(eq(regionMapping));

    Throwable throwable =
        catchThrowable(() -> function.createRegionMapping(service, regionMapping));

    assertThat(throwable).isInstanceOf(RegionMappingExistsException.class);
    verify(service, times(1)).createRegionMapping(regionMapping);
  }

  @Test
  public void createRegionMappingThrowsIfRegionDoesNotExist() {
    when(cache.getRegion(REGION_NAME)).thenReturn(null);

    Throwable throwable = catchThrowable(() -> function.executeFunction(context));

    assertThat(throwable).isInstanceOf(IllegalStateException.class)
        .hasMessage("create jdbc-mapping requires that the region \"" + REGION_NAME + "\" exists.");
  }

  @Test
  public void executeCreatesMapping() throws Exception {
    function.executeFunction(context);

    verify(service, times(1)).createRegionMapping(regionMapping);
  }

  @Test
  public void executeAlterRegionLoader() throws Exception {
    function.executeFunction(context);

    verify(service, times(1)).createRegionMapping(regionMapping);
    AttributesMutator<Object, Object> mutator = region.getAttributesMutator();
    verify(mutator, times(1)).setCacheLoader(any());
  }

  @Test
  public void executeWithSynchronousAltersRegionWriter() throws Exception {
    setupSynchronous();
    function.executeFunction(context);

    AttributesMutator<Object, Object> mutator = region.getAttributesMutator();
    verify(mutator, times(1)).setCacheWriter(any());
  }

  @Test
  public void executeWithSynchronousNeverAltersRegionAsyncEventQueue() throws Exception {
    setupSynchronous();
    function.executeFunction(context);

    AttributesMutator<Object, Object> mutator = region.getAttributesMutator();
    verify(mutator, never()).addAsyncEventQueueId(any());
  }

  @Test
  public void executeWithSynchronousNeverCreatesAsyncQueue() throws Exception {
    setupSynchronous();
    function.executeFunction(context);

    verify(asyncEventQueueFactory, never()).create(any(), any());
  }

  @Test
  public void executeAlterRegionAsyncEventQueue() throws Exception {
    String queueName = MappingCommandUtils.createAsyncEventQueueName(REGION_NAME);
    function.executeFunction(context);

    verify(service, times(1)).createRegionMapping(regionMapping);
    AttributesMutator<Object, Object> mutator = region.getAttributesMutator();
    verify(mutator, times(1)).addAsyncEventQueueId(queueName);
  }

  @Test
  public void executeCreatesSerialAsyncQueueForNonPartitionedRegion() throws Exception {
    when(attributes.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    function.executeFunction(context);

    verify(asyncEventQueueFactory, times(1)).create(any(), any());
    verify(asyncEventQueueFactory, times(1)).setParallel(false);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldOnlyAddAEQIdForProxyPR() throws Exception {
    when(attributes.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    PartitionedRegion pr = mock(PartitionedRegion.class);
    when(cache.getRegion(REGION_NAME)).thenReturn(pr);
    when(pr.getAttributes()).thenReturn(attributes);
    PartitionAttributes<Object, Object> pa = mock(PartitionAttributes.class);
    when(attributes.getPartitionAttributes()).thenReturn(pa);
    when(pa.getLocalMaxMemory()).thenReturn(0);
    when(pr.getAttributesMutator()).thenReturn(mock(AttributesMutator.class));
    function.executeFunction(context);

    @SuppressWarnings("unchecked")
    AttributesMutator<Object, Object> mutator = pr.getAttributesMutator();
    verify(mutator, times(1)).addAsyncEventQueueId(any());

    verify(mutator, times(0)).setCacheWriter(any());
    verify(mutator, times(0)).setCacheLoader(any());
    verify(asyncEventQueueFactory, times(0)).create(any(), any());
  }

  @Test
  public void shouldOnlyAddAEQIdForProxyReplicate() throws Exception {
    when(attributes.getDataPolicy()).thenReturn(DataPolicy.EMPTY);
    when(cache.getRegion(REGION_NAME)).thenReturn(region);
    LocalRegion lr = (LocalRegion) region;
    when(lr.getAttributes()).thenReturn(attributes);
    when(lr.getAttributesMutator()).thenReturn(mock(AttributesMutator.class));
    function.executeFunction(context);

    @SuppressWarnings("unchecked")
    AttributesMutator<Object, Object> mutator = lr.getAttributesMutator();
    verify(mutator, times(1)).addAsyncEventQueueId(any());

    verify(mutator, times(0)).setCacheWriter(any());
    verify(mutator, times(0)).setCacheLoader(any());
    verify(asyncEventQueueFactory, times(0)).create(any(), any());
  }

  @Test
  public void executeCreatesParallelAsyncQueueForPartitionedRegion() throws Exception {
    when(attributes.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    function.executeFunction(context);

    verify(asyncEventQueueFactory, times(1)).create(any(), any());
    verify(asyncEventQueueFactory, times(1)).setParallel(true);
  }

  @Test
  public void executeCreatesPartitionedProxyShouldNotContainWriterOrLoader() throws Exception {
    when(attributes.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    function.executeFunction(context);

    verify(asyncEventQueueFactory, times(1)).create(any(), any());
    verify(asyncEventQueueFactory, times(1)).setParallel(true);
  }

  @Test
  public void executeReportsErrorIfRegionMappingExists() throws Exception {
    doThrow(RegionMappingExistsException.class).when(service)
        .createRegionMapping(eq(regionMapping));

    function.execute(context);

    ArgumentCaptor<CliFunctionResult> argument = ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender, times(1)).lastResult(argument.capture());
    assertThat(argument.getValue().getStatusMessage())
        .contains(RegionMappingExistsException.class.getName());
  }
}
