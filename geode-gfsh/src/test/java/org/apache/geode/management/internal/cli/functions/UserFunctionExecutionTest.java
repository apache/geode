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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.security.AuthenticationRequiredException;

public class UserFunctionExecutionTest {
  private Object[] arguments;
  private Execution execution;
  private Function userFunction;
  private UserFunctionExecution function;
  private SecurityService securityService;
  private ResultCollector resultCollector;
  private FunctionContext<Object[]> context;
  private ResultSender<Object> resultSender;
  private InternalCacheForClientAccess filterCache;
  private ArgumentCaptor<CliFunctionResult> resultCaptor;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    execution = mock(Execution.class);
    userFunction = mock(Function.class);
    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    function = spy(UserFunctionExecution.class);
    securityService = mock(SecurityService.class);
    resultCollector = mock(ResultCollector.class);
    filterCache = mock(InternalCacheForClientAccess.class);
    arguments = new Object[] {"TestFunction", "key1,key2", "TestResultCollector", "arg1,arg2",
        SEPARATOR + "TestRegion", new Properties()};

    when(userFunction.getId()).thenReturn("TestFunction");

    InternalCache cache = mock(InternalCache.class);
    DistributedSystem distributedSystem = mock(InternalDistributedSystem.class);
    DistributedMember distributedMember = mock(InternalDistributedMember.class);
    when(distributedMember.getId()).thenReturn("MockMemberId");
    when(distributedSystem.getDistributedMember()).thenReturn(distributedMember);
    when(filterCache.getDistributedSystem()).thenReturn(distributedSystem);

    when(cache.getSecurityService()).thenReturn(securityService);
    when(cache.getCacheForProcessingClientRequests()).thenReturn(filterCache);
    when(context.getCache()).thenReturn(cache);
    when(context.getArguments()).thenReturn(arguments);
    when(context.getResultSender()).thenReturn(resultSender);

    when(execution.withFilter(any())).thenReturn(execution);
    when(execution.setArguments(any())).thenReturn(execution);
    when(execution.withCollector(any())).thenReturn(execution);
    when(execution.execute(anyString())).thenReturn(resultCollector);

    doReturn(false).when(function).loginRequired(securityService);
    doReturn(userFunction).when(function).loadFunction("TestFunction");
    doReturn(resultCollector).when(function).parseResultCollector("TestResultCollector");
    doReturn(execution).when(function).buildExecution(any(), any());

    resultCaptor = ArgumentCaptor.forClass(CliFunctionResult.class);
  }

  @Test
  public void testDefaultAttributes() {
    assertThat(function.isHA()).isFalse();
    assertThat(function.getId()).isEqualTo(UserFunctionExecution.ID);
    assertThat(function.getRequiredPermissions(anyString())).isEmpty();
  }

  @Test
  public void parseArgumentsTest() {
    assertThat(function.parseArguments(null)).isNull();
    assertThat(function.parseArguments("")).isNull();
    assertThat(function.parseArguments("arg1,arg2")).isNotNull()
        .isEqualTo(new String[] {"arg1", "arg2"});
  }

  @Test
  public void parseFiltersTest() {
    assertThat(function.parseFilters(null)).isNotNull().isEmpty();
    assertThat(function.parseFilters("")).isNotNull().isEmpty();
    assertThat(function.parseFilters("key1,key2")).isNotNull().containsOnly("key1", "key2");
  }

  @Test
  public void buildExecutionShouldThrowExceptionWhenRegionIsRequiredButItDoesNotExist()
      throws Exception {
    when(filterCache.getRegion("region")).thenReturn(null);
    when(function.buildExecution(any(), any())).thenCallRealMethod();

    assertThatThrownBy(() -> function.buildExecution(filterCache, "region"))
        .isInstanceOf(RegionNotFoundException.class);
  }

  @Test
  public void loginRequiredShouldReturnTrueWhenSubjectIsNull() {
    when(securityService.getSubject()).thenReturn(null);
    when(function.loginRequired(securityService)).thenCallRealMethod();

    assertThat(function.loginRequired(securityService)).isTrue();
  }

  @Test
  public void loginRequiredShouldReturnTrueWhenSubjectIsNotAuthenticated() {
    Subject subject = mock(Subject.class);
    when(subject.isAuthenticated()).thenReturn(false);
    when(securityService.getSubject()).thenReturn(subject);
    when(function.loginRequired(securityService)).thenCallRealMethod();

    assertThat(function.loginRequired(securityService)).isTrue();
  }

  @Test
  public void loginRequiredShouldReturnTrueWhenSecurityServiceFailsToLoadSubject() {
    when(function.loginRequired(securityService)).thenCallRealMethod();
    doThrow(new AuthenticationRequiredException("Dummy Exception")).when(securityService)
        .getSubject();

    assertThat(function.loginRequired(securityService)).isTrue();
  }

  @Test
  public void loginRequiredShouldReturnFalseWhenSubjectIsAuthenticated() {
    Subject subject = mock(Subject.class);
    when(subject.isAuthenticated()).thenReturn(true);
    when(securityService.getSubject()).thenReturn(subject);
    when(function.loginRequired(securityService)).thenCallRealMethod();

    assertThat(function.loginRequired(securityService)).isFalse();
  }

  @Test
  public void executeShouldFailWhenNoArgumentsAreProvided() {
    when(context.getArguments()).thenReturn(null);

    function.execute(context);

    verify(resultSender, times(1)).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage()).isEqualTo("Could not retrieve arguments");
  }

  @Test
  public void executeShouldFailWhenTargetFunctionCanNotBeLoaded() {
    doReturn(null).when(function).loadFunction(anyString());

    function.execute(context);

    verify(resultSender, times(1)).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage())
        .isEqualTo("Function : TestFunction is not registered on member.");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void executeShouldFailWhenResultCollectorCanNotBeInstantiated() throws Exception {
    CliFunctionResult result;

    doThrow(new ClassNotFoundException("ClassNotFoundException")).when(function)
        .parseResultCollector(anyString());
    function.execute(context);
    verify(resultSender, times(1)).lastResult(resultCaptor.capture());
    result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage()).isEqualTo(
        "ResultCollector : TestResultCollector not found. Error : ClassNotFoundException");
    reset(resultSender);

    doThrow(new IllegalAccessException("IllegalAccessException")).when(function)
        .parseResultCollector(anyString());
    function.execute(context);
    verify(resultSender, times(1)).lastResult(resultCaptor.capture());
    result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage()).isEqualTo(
        "ResultCollector : TestResultCollector not found. Error : IllegalAccessException");
    reset(resultSender);

    doThrow(new InstantiationException("InstantiationException")).when(function)
        .parseResultCollector(anyString());
    function.execute(context);
    verify(resultSender, times(1)).lastResult(resultCaptor.capture());
    result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage()).isEqualTo(
        "ResultCollector : TestResultCollector not found. Error : InstantiationException");
    reset(resultSender);
  }

  @Test
  public void executeShouldFailWhenRegionIsSetAsArgumentButItDoesNotExist() throws Exception {
    when(function.buildExecution(any(), any())).thenCallRealMethod();
    function.execute(context);

    verify(resultSender, times(1)).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage()).isEqualTo(SEPARATOR + "TestRegion does not exist");
  }

  @Test
  public void executeShouldFailWhenExecutorCanNotBeLoaded() throws Exception {
    doReturn(null).when(function).buildExecution(any(), any());

    function.execute(context);

    verify(resultSender, times(1)).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusMessage()).isEqualTo(
        "While executing function : TestFunction on member : MockMemberId one region : " + SEPARATOR
            + "TestRegion error occurred : Could not retrieve executor");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void executeShouldProperlyConfigureExecutionContext() {
    Set<String> filter = new HashSet<>();
    filter.add("key1");
    filter.add("key2");

    arguments = new Object[] {"TestFunction", "key1,key2", "TestResultCollector", "arg1,arg2",
        SEPARATOR + "TestRegion", new Properties()};
    when(context.getArguments()).thenReturn(arguments);
    function.execute(context);
    verify(execution, times(1)).withFilter(filter);
    verify(execution, times(1)).withCollector(resultCollector);
    verify(execution, times(1)).setArguments(new String[] {"arg1", "arg2"});
    verify(resultSender, times(1)).lastResult(resultCaptor.capture());
    CliFunctionResult resultFullArguments = resultCaptor.getValue();
    assertThat(resultFullArguments.isSuccessful()).isTrue();

    reset(resultSender);
    reset(execution);
    arguments = new Object[] {"TestFunction", "", "", "", "", new Properties()};
    when(context.getArguments()).thenReturn(arguments);
    function.execute(context);
    verify(execution, never()).withFilter(any());
    verify(execution, never()).setArguments(any());
    verify(execution, never()).withCollector(any());
    verify(resultSender, times(1)).lastResult(resultCaptor.capture());
    CliFunctionResult resultNoArguments = resultCaptor.getValue();
    assertThat(resultNoArguments.isSuccessful()).isTrue();
  }

  @Test
  public void executeShouldWorkProperlyForFunctionsWithResults() {
    when(userFunction.hasResult()).thenReturn(true);
    doReturn(true).when(function).loginRequired(any());
    when(resultCollector.getResult()).thenReturn(Arrays.asList("result1", "result2"));

    function.execute(context);
    verify(resultSender, times(1)).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusMessage()).isEqualTo("[result1, result2]");
    verify(securityService, times(1)).login(any());
    verify(securityService, times(1)).logout();
  }

  @Test
  public void executeShouldWorkProperlyForFunctionsWithoutResults() {
    when(userFunction.hasResult()).thenReturn(false);
    doReturn(true).when(function).loginRequired(any());

    function.execute(context);
    verify(resultSender, times(1)).lastResult(resultCaptor.capture());
    CliFunctionResult result = resultCaptor.getValue();
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusMessage()).isEqualTo("[]");
    verify(securityService, times(1)).login(any());
    verify(securityService, times(1)).logout();
  }
}
