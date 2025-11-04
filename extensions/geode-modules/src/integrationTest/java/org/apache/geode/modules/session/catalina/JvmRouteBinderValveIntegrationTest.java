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
package org.apache.geode.modules.session.catalina;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.UUID;

import jakarta.servlet.ServletException;
import junitparams.Parameters;
import org.apache.catalina.Context;
import org.apache.catalina.Manager;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class JvmRouteBinderValveIntegrationTest extends AbstractSessionValveIntegrationTest {
  private Request request;
  private Response response;
  private TestValve testValve;
  private JvmRouteBinderValve jvmRouteBinderValve;

  @Before
  public void setUp() {
    // Tomcat 10+: Use mock() instead of spy() to avoid Tomcat Request/Response constructor
    // complexities
    request = mock(Request.class);
    response = mock(Response.class);
    testValve = new TestValve(false);

    jvmRouteBinderValve = new JvmRouteBinderValve();
    jvmRouteBinderValve.setNext(testValve);
  }

  protected void parameterizedSetUp(RegionShortcut regionShortcut) {
    super.parameterizedSetUp(regionShortcut);
    Context mockContext = mock(Context.class);
    // Tomcat 10+: Mock context configuration to satisfy Jakarta Servlet lifecycle requirements
    when(mockContext.getApplicationLifecycleListeners()).thenReturn(new Object[0]);
    when(mockContext.getDistributable()).thenReturn(false);
    // Configure bidirectional manager-context relationship for session management
    when(mockContext.getManager()).thenReturn(deltaSessionManager);
    when(deltaSessionManager.getContext()).thenReturn(mockContext);
    when(request.getContext()).thenReturn(mockContext);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void invokeShouldCallNextChainedValveAndDoNothingWhenSessionManagerDoesNotBelongToGeode(
      RegionShortcut regionShortcut) throws IOException, ServletException {
    parameterizedSetUp(regionShortcut);
    when(request.getContext().getManager()).thenReturn(mock(Manager.class));

    jvmRouteBinderValve.invoke(request, response);
    assertThat(testValve.invocations.get()).isEqualTo(1);
    verify(deltaSessionManager, times(0)).getJvmRoute();
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void invokeShouldCallNextChainedValveAndDoNothingWhenSessionManagerBelongsToGeodeButJvmRouteIsNull(
      RegionShortcut regionShortcut) throws IOException, ServletException {
    parameterizedSetUp(regionShortcut);
    doReturn(null).when(deltaSessionManager).getJvmRoute();
    when(request.getContext().getManager()).thenReturn(deltaSessionManager);

    jvmRouteBinderValve.invoke(request, response);
    assertThat(testValve.invocations.get()).isEqualTo(1);
    verify(request, times(0)).getRequestedSessionId();
    verify(deltaSessionManager, times(1)).getJvmRoute();
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void invokeShouldNotFailoverWhenRequestedSessionIdIsNull(RegionShortcut regionShortcut)
      throws IOException, ServletException {
    parameterizedSetUp(regionShortcut);
    when(deltaSessionManager.getJvmRoute()).thenReturn("");
    when(request.getRequestedSessionId()).thenReturn(null);
    when(request.getContext().getManager()).thenReturn(deltaSessionManager);

    jvmRouteBinderValve.invoke(request, response);
    assertThat(testValve.invocations.get()).isEqualTo(1);
    verify(request, times(0)).changeSessionId(anyString());
    assertThat(httpSessionRegion.get(TEST_SESSION_ID).getId()).isEqualTo(TEST_SESSION_ID);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void invokeShouldNotFailoverWhenJvmRouteWithinTheRequestedSessionIdIsNull(
      RegionShortcut regionShortcut) throws IOException, ServletException {
    parameterizedSetUp(regionShortcut);
    when(deltaSessionManager.getJvmRoute()).thenReturn("");
    when(request.getContext().getManager()).thenReturn(deltaSessionManager);
    when(request.getRequestedSessionId()).thenReturn(UUID.randomUUID().toString());

    jvmRouteBinderValve.invoke(request, response);
    assertThat(testValve.invocations.get()).isEqualTo(1);
    verify(request, times(0)).changeSessionId(anyString());
    assertThat(httpSessionRegion.get(TEST_SESSION_ID).getId()).isEqualTo(TEST_SESSION_ID);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void invokeShouldNotFailoverWhenJvmRouteWithinTheRequestedSessionIsSameAsLocalJvmRoute(
      RegionShortcut regionShortcut) throws IOException, ServletException {
    parameterizedSetUp(regionShortcut);
    when(deltaSessionManager.getJvmRoute()).thenReturn(TEST_JVM_ROUTE);
    when(request.getRequestedSessionId()).thenReturn(TEST_SESSION_ID);
    when(request.getContext().getManager()).thenReturn(deltaSessionManager);

    jvmRouteBinderValve.invoke(request, response);
    assertThat(testValve.invocations.get()).isEqualTo(1);
    verify(request, times(0)).changeSessionId(anyString());
    assertThat(httpSessionRegion.get(TEST_SESSION_ID).getId()).isEqualTo(TEST_SESSION_ID);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void invokeShouldNotFailoverWhenJvmRoutesDifferAndManagerCanNotFindOriginalSession(
      RegionShortcut regionShortcut) throws IOException, ServletException {
    parameterizedSetUp(regionShortcut);
    when(deltaSessionManager.getJvmRoute()).thenReturn("jvmRoute");
    doCallRealMethod().when(deltaSessionManager).findSession(anyString());
    when(request.getContext().getManager()).thenReturn(deltaSessionManager);
    when(request.getRequestedSessionId()).thenReturn("nonExistingSessionId.anotherJvmRoute");

    jvmRouteBinderValve.invoke(request, response);
    assertThat(testValve.invocations.get()).isEqualTo(1);
    verify(request, times(0)).changeSessionId(anyString());
    assertThat(httpSessionRegion.get(TEST_SESSION_ID).getId()).isEqualTo(TEST_SESSION_ID);
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION"})
  public void invokeShouldCorrectlyHandleSessionFailover(RegionShortcut regionShortcut)
      throws IOException, ServletException {
    parameterizedSetUp(regionShortcut);
    when(deltaSessionManager.getJvmRoute()).thenReturn("jvmRoute");
    when(deltaSessionManager.getContextName()).thenReturn(TEST_CONTEXT);
    Context mockContext = mock(Context.class);
    // Tomcat 10+: Configure lifecycle listeners for Jakarta Servlet session creation events
    when(mockContext.getApplicationLifecycleListeners())
        .thenReturn(new Object[] {});
    when(deltaSessionManager.getTheContext()).thenReturn(mockContext);
    doCallRealMethod().when(deltaSessionManager).findSession(anyString());

    when(request.getRequestedSessionId()).thenReturn(TEST_SESSION_ID);
    when(request.getContext().getManager()).thenReturn(deltaSessionManager);

    jvmRouteBinderValve.invoke(request, response);
    String expectedFailoverSessionId =
        TEST_SESSION_ID.substring(0, TEST_SESSION_ID.indexOf(".") + 1) + "jvmRoute";
    assertThat(testValve.invocations.get()).isEqualTo(1);
    verify(request, times(1)).changeSessionId(expectedFailoverSessionId);
    assertThat(httpSessionRegion.get(TEST_SESSION_ID).getId()).isEqualTo(expectedFailoverSessionId);
  }
}
