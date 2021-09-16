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

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class WanCopyRegionFunctionTest {

  private WanCopyRegionFunction function;
  private InternalCache internalCacheMock;
  private GatewaySender gatewaySenderMock;

  private final FunctionContext<Object[]> contextMock = uncheckedCast(mock(FunctionContext.class));

  private final Region<?, ?> regionMock =
      uncheckedCast(mock(InternalRegion.class));


  @Before
  public void setUp() throws InterruptedException {
    gatewaySenderMock = mock(AbstractGatewaySender.class);
    when(gatewaySenderMock.getId()).thenReturn("mySender");

    function = new WanCopyRegionFunction();

    RegionAttributes<?, ?> attributesMock = mock(RegionAttributes.class);
    Set<?> idsMock = mock(Set.class);
    when(idsMock.contains(anyString())).thenReturn(true);
    when(attributesMock.getGatewaySenderIds()).thenReturn(uncheckedCast(idsMock));
    when(regionMock.getAttributes()).thenReturn(uncheckedCast(attributesMock));

    internalCacheMock = mock(InternalCache.class);
    when(internalCacheMock.getRegion(any())).thenReturn(uncheckedCast(regionMock));
  }

  @Test
  public void executeFunction_verifyErrorWhenRegionNotFound() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(internalCacheMock.getRegion(any())).thenReturn(null);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    CliFunctionResult result = function.executeFunction(contextMock);

    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo("Region myRegion not found");
  }

  @Test
  public void executeFunction_verifyErrorWhenSenderNotFound() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(internalCacheMock.getGatewaySender(any())).thenReturn(null);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    CliFunctionResult result = function.executeFunction(contextMock);

    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo("Sender mySender not found");
  }

  @Test
  public void executeFunction_verifyErrorWhenSenderIsNotRunning() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(gatewaySenderMock.isRunning()).thenReturn(false);
    when(internalCacheMock.getGatewaySender(any())).thenReturn(gatewaySenderMock);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    CliFunctionResult result = function.executeFunction(contextMock);

    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage()).isEqualTo("Sender mySender is not running");
  }

  @Test
  public void executeFunction_verifySuccessWhenSenderIsSerialAndSenderIsNotPrimary() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    when(gatewaySenderMock.isRunning()).thenReturn(true);
    when(gatewaySenderMock.isParallel()).thenReturn(false);
    when(((InternalGatewaySender) gatewaySenderMock).isPrimary()).thenReturn(false);
    when(internalCacheMock.getGatewaySender(any())).thenReturn(gatewaySenderMock);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    CliFunctionResult result = function.executeFunction(contextMock);

    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.OK.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Sender mySender is serial and not primary. 0 entries copied.");
  }

  @Test
  public void executeFunction_verifyErrorWhenSenderNotConfiguredWithForRegion() {
    Object[] options = new Object[] {"myRegion", "mySender", false, 1L, 10};
    Set<String> senders = new HashSet<>();
    senders.add("notMySender");
    when(gatewaySenderMock.isParallel()).thenReturn(true);
    when(regionMock.getAttributes().getGatewaySenderIds()).thenReturn(senders);
    when(internalCacheMock.getGatewaySender(any())).thenReturn(gatewaySenderMock);
    when(contextMock.getArguments()).thenReturn(options);
    when(contextMock.getCache()).thenReturn(internalCacheMock);

    CliFunctionResult result = function.executeFunction(contextMock);
    assertThat(result.getStatus()).isEqualTo(CliFunctionResult.StatusState.ERROR.toString());
    assertThat(result.getStatusMessage())
        .isEqualTo("Region myRegion is not configured to use sender mySender");
  }
}
