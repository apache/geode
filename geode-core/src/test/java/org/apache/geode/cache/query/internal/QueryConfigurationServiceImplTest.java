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
package org.apache.geode.cache.query.internal;

import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY;
import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.CONTINUOUS_QUERIES_RUNNING_MESSAGE;
import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.INTERFACE_NOT_IMPLEMENTED_MESSAGE;
import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.NULL_CACHE_ERROR_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.runner.RunWith;

import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.cache.query.security.JavaBeanAccessorMethodAuthorizer;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RegExMethodAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.cli.util.TestMethodAuthorizer;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class QueryConfigurationServiceImplTest {
  private static final Set<String> EMPTY_SET = Collections.emptySet();
  private CqService mockCqService;
  private InternalCache mockCache;
  private SecurityService mockSecurity;
  private QueryConfigurationServiceImpl configService;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    mockCqService = mock(CqService.class);
    when(mockCqService.getAllCqs()).thenReturn(Collections.emptyList());

    mockSecurity = mock(SecurityService.class);
    configService = spy(new QueryConfigurationServiceImpl());

    mockCache = mock(InternalCache.class);
    when(mockCache.getCqService()).thenReturn(mockCqService);
    when(mockCache.getSecurityService()).thenReturn(mockSecurity);
  }

  @SuppressWarnings("unused")
  private Object[] getMethodAuthorizerClasses() {
    return new Object[] {
        RegExMethodAuthorizer.class,
        RestrictedMethodAuthorizer.class,
        UnrestrictedMethodAuthorizer.class,
        JavaBeanAccessorMethodAuthorizer.class,
    };
  }

  @SuppressWarnings("deprecation")
  private void setAllowUntrustedMethodInvocationSystemProperty() {
    System.setProperty(ALLOW_UNTRUSTED_METHOD_INVOCATION_SYSTEM_PROPERTY, "true");
  }

  @Test
  public void initThrowsExceptionWhenCacheIsNull() {
    assertThatThrownBy(() -> configService.init(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(NULL_CACHE_ERROR_MESSAGE);
  }

  @Test
  public void initSetsNoOpAuthorizerWhenSecurityDisabled() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(false);
    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer())
        .isSameAs(QueryConfigurationServiceImpl.getNoOpAuthorizer());
  }

  @Test
  public void initSetsNoOpAuthorizerWhenSystemPropertyIsSet() {
    setAllowUntrustedMethodInvocationSystemProperty();
    configService = new QueryConfigurationServiceImpl();
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);
    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer())
        .isSameAs(QueryConfigurationServiceImpl.getNoOpAuthorizer());
  }

  @Test
  public void initSetsRestrictedMethodAuthorizerWhenSecurityIsEnabledAndSystemPropertyIsNotSet() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);
    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
  }

  @Test
  public void updateMethodAuthorizerDoesNothingWhenSecurityIsDisabled() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(false);
    configService.init(mockCache);
    MethodInvocationAuthorizer authorizer = configService.getMethodAuthorizer();
    assertThat(authorizer).isSameAs(QueryConfigurationServiceImpl.getNoOpAuthorizer());

    configService.updateMethodAuthorizer(mockCache, false,
        RestrictedMethodAuthorizer.class.getName(), EMPTY_SET);
    assertThat(configService.getMethodAuthorizer()).isSameAs(authorizer);
  }

  @Test
  public void updateMethodAuthorizerDoesNothingWhenSystemPropertyIsSet() {
    setAllowUntrustedMethodInvocationSystemProperty();
    configService = new QueryConfigurationServiceImpl();
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);
    configService.init(mockCache);

    MethodInvocationAuthorizer authorizer = configService.getMethodAuthorizer();
    assertThat(authorizer).isSameAs(QueryConfigurationServiceImpl.getNoOpAuthorizer());

    configService.updateMethodAuthorizer(mockCache, false,
        RestrictedMethodAuthorizer.class.getName(), EMPTY_SET);
    assertThat(configService.getMethodAuthorizer()).isSameAs(authorizer);
  }

  @Test
  @TestCaseName("{method} Authorizer={0}")
  @Parameters(method = "getMethodAuthorizerClasses")
  public void updateMethodAuthorizerSetsCorrectAuthorizer(Class methodAuthorizerClass) {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);

    configService.updateMethodAuthorizer(mockCache, false, methodAuthorizerClass.getName(),
        EMPTY_SET);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(methodAuthorizerClass);
  }

  @Test
  public void updateMethodAuthorizerSetsUserSpecifiedAuthorizer() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);
    String testAuthorizerName = TestMethodAuthorizer.class.getName();
    Set<String> parameters = new HashSet<>();
    String testParam1 = "test1";
    String testParam2 = "test2";
    parameters.add(testParam1);
    parameters.add(testParam2);

    configService.updateMethodAuthorizer(mockCache, false, testAuthorizerName, parameters);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(TestMethodAuthorizer.class);
    TestMethodAuthorizer methodAuthorizer =
        (TestMethodAuthorizer) configService.getMethodAuthorizer();
    assertThat(methodAuthorizer.getParameters()).isEqualTo(parameters);
  }

  @Test
  public void updateMethodAuthorizerDoesNotChangeMethodAuthorizerWhenSecurityIsEnabledAndClassNameIsNull() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);

    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    assertThatThrownBy(
        () -> configService.updateMethodAuthorizer(mockCache, false, null, EMPTY_SET))
            .isInstanceOf(QueryConfigurationServiceException.class);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
  }

  @Test
  public void updateMethodAuthorizerDoesNotChangeMethodAuthorizerWhenSecurityIsEnabledAndClassNameIsNotFound() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);

    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    String className = "FakeClassName";
    assertThatThrownBy(
        () -> configService.updateMethodAuthorizer(mockCache, false, className, EMPTY_SET))
            .isInstanceOf(QueryConfigurationServiceException.class);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
  }

  @Test
  public void updateMethodAuthorizerDoesNotChangeMethodAuthorizerWhenSecurityIsEnabledAndSpecifiedClassDoesNotImplementMethodInvocationAuthorizer() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);

    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    String className = this.getClass().getName();
    assertThatThrownBy(
        () -> configService.updateMethodAuthorizer(mockCache, false, className, EMPTY_SET))
            .hasCauseInstanceOf(QueryConfigurationServiceException.class)
            .hasStackTraceContaining(String.format(INTERFACE_NOT_IMPLEMENTED_MESSAGE, className,
                MethodInvocationAuthorizer.class.getName()));
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
  }

  @Test
  public void updateMethodAuthorizerDoesNotChangeMethodAuthorizerWhenSecurityIsEnabledAndSpecifiedClassCannotBeInstantiated() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);
    when(mockCache.isClosed()).thenThrow(new RuntimeException("Test exception"));

    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    assertThatThrownBy(() -> configService.updateMethodAuthorizer(mockCache, false,
        TestMethodAuthorizer.class.getName(), EMPTY_SET))
            .isInstanceOf(QueryConfigurationServiceException.class);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
  }

  @Test
  @TestCaseName("{method} Authorizer={0}")
  @Parameters(method = "getMethodAuthorizerClasses")
  public void updateMethodAuthorizerDoesNotChangeMethodAuthorizerAndThrowsExceptionWhenCqsAreRunningAndForceUpdateFlagIsSetAsFalse(
      Class methodAuthorizerClass) {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);
    doReturn(Collections.singletonList(mock(ServerCQ.class))).when(mockCqService).getAllCqs();
    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);

    assertThatThrownBy(() -> configService.updateMethodAuthorizer(mockCache, false,
        methodAuthorizerClass.getName(), EMPTY_SET))
            .isInstanceOf(QueryConfigurationServiceException.class)
            .hasMessage(CONTINUOUS_QUERIES_RUNNING_MESSAGE);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
  }

  @Test
  @TestCaseName("{method} Authorizer={0}")
  @Parameters(method = "getMethodAuthorizerClasses")
  public void updateMethodAuthorizerChangesMethodAuthorizerAndInvalidatesCqsCacheWhenCqsAreRunningAndForceUpdateFlagIsSetAsTrue(
      Class methodAuthorizerClass) {
    ServerCQ serverCQ1 = mock(ServerCQ.class);
    ServerCQ serverCQ2 = mock(ServerCQ.class);
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);
    doReturn(Arrays.asList(serverCQ1, serverCQ2)).when(mockCqService).getAllCqs();
    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);

    assertThatCode(() -> configService.updateMethodAuthorizer(mockCache, true,
        methodAuthorizerClass.getName(), EMPTY_SET)).doesNotThrowAnyException();
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(methodAuthorizerClass);
    verify(serverCQ1, times(1)).invalidateCqResultKeys();
    verify(serverCQ2, times(1)).invalidateCqResultKeys();
  }
}
