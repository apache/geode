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

import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.AUTHORIZER_NOT_UPDATED;
import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.INSTANTIATION_ERROR;
import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.NO_CLASS_FOUND;
import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.NO_VALID_CONSTRUCTOR;
import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.NULL_CLASS_NAME;
import static org.apache.geode.cache.query.internal.QueryConfigurationServiceImpl.UPDATE_ERROR_MESSAGE;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.query.security.JavaBeanAccessorMethodAuthorizer;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RegExMethodAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer;
import org.apache.geode.cache.util.TestMethodAuthorizer;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;

@RunWith(JUnitParamsRunner.class)
public class QueryConfigurationServiceImplTest {
  private InternalCache mockCache;
  private SecurityService mockSecurity;
  private QueryConfigurationServiceImpl configService;
  private DistributedLockService mockDistributedLockService;

  @Before
  public void setUp() {
    mockCache = mock(InternalCache.class);
    mockSecurity = mock(SecurityService.class);
    when(mockCache.getSecurityService()).thenReturn(mockSecurity);
    configService = spy(new QueryConfigurationServiceImpl());
    mockDistributedLockService = mock(DistributedLockService.class);
  }

  @Test
  public void initThrowsExceptionWhenCacheIsNull() {
    assertThatThrownBy(() -> configService.init(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void queryConfigurationServiceUsesNoOpAuthorizerWithSecurityDisabled() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(false);
    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer())
        .isSameAs(QueryConfigurationServiceImpl.getNoOpAuthorizer());
  }

  @Test
  public void queryConfigurationServiceUsesNoOpAuthorizerWhenSystemPropertyIsSet() {
    String propertyKey = GEMFIRE_PREFIX + "QueryService.allowUntrustedMethodInvocation";
    try {
      System.setProperty(propertyKey, "true");
      configService = new QueryConfigurationServiceImpl();
      when(mockSecurity.isIntegratedSecurity()).thenReturn(true);
      configService.init(mockCache);
      assertThat(configService.getMethodAuthorizer())
          .isSameAs(QueryConfigurationServiceImpl.getNoOpAuthorizer());
    } finally {
      System.clearProperty(propertyKey);
    }
  }

  @Test
  public void queryConfigurationServiceUsesRestrictedMethodAuthorizerWhenSecurityIsEnabled() {
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

    configService.updateMethodAuthorizer(mockCache,
        RestrictedMethodAuthorizer.class.getName(), new HashSet<>());

    assertThat(configService.getMethodAuthorizer()).isSameAs(authorizer);
  }

  @Test
  public void updateMethodAuthorizerDoesNothingWhenSystemPropertyIsSet() {
    String propertyKey = GEMFIRE_PREFIX + "QueryService.allowUntrustedMethodInvocation";
    try {
      System.setProperty(propertyKey, "true");
      configService = new QueryConfigurationServiceImpl();
      when(mockSecurity.isIntegratedSecurity()).thenReturn(true);
      configService.init(mockCache);

      MethodInvocationAuthorizer authorizer = configService.getMethodAuthorizer();

      assertThat(authorizer).isSameAs(QueryConfigurationServiceImpl.getNoOpAuthorizer());

      configService.updateMethodAuthorizer(mockCache,
          RestrictedMethodAuthorizer.class.getName(), new HashSet<>());

      assertThat(configService.getMethodAuthorizer()).isSameAs(authorizer);
    } finally {
      System.clearProperty(propertyKey);
    }
  }

  @Test
  @TestCaseName("{method} Authorizer={0}")
  @Parameters(method = "getMethodAuthorizerClasses")
  public void updateMethodAuthorizerSetsCorrectAuthorizer(Class methodAuthorizerClass) {
    doReturn(mockDistributedLockService).when(configService).getLockService(mockCache);
    when(mockDistributedLockService.lock(any(String.class), any(long.class), any(long.class)))
        .thenReturn(true);

    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);

    configService.updateMethodAuthorizer(mockCache, methodAuthorizerClass.getName(),
        new HashSet<>());
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

    doReturn(mockDistributedLockService).when(configService).getLockService(mockCache);
    when(mockDistributedLockService.lock(any(String.class), any(long.class), any(long.class)))
        .thenReturn(true);

    configService.updateMethodAuthorizer(mockCache, testAuthorizerName, parameters);

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
    configService.updateMethodAuthorizer(mockCache, null, new HashSet<>());
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    verify(configService).logError(
        eq(UPDATE_ERROR_MESSAGE + NULL_CLASS_NAME + AUTHORIZER_NOT_UPDATED),
        any(NullPointerException.class));
  }

  @Test
  public void updateMethodAuthorizerDoesNotChangeMethodAuthorizerWhenSecurityIsEnabledAndClassNameIsNotFound() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);

    doReturn(mockDistributedLockService).when(configService).getLockService(mockCache);
    when(mockDistributedLockService.lock(any(String.class), any(long.class), any(long.class)))
        .thenReturn(true);

    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    String className = "FakeClassName";
    configService.updateMethodAuthorizer(mockCache, className, new HashSet<>());
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    verify(configService).logError(
        eq(UPDATE_ERROR_MESSAGE + NO_CLASS_FOUND + className + ". " + AUTHORIZER_NOT_UPDATED),
        any(ClassNotFoundException.class));
  }

  @Test
  public void updateMethodAuthorizerDoesNotChangeMethodAuthorizerWhenSecurityIsEnabledAndSpecifiedClassHasNoValidConstructor() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);

    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    configService.updateMethodAuthorizer(mockCache, this.getClass().getName(), new HashSet<>());
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    verify(configService).logError(
        eq(UPDATE_ERROR_MESSAGE + NO_VALID_CONSTRUCTOR + AUTHORIZER_NOT_UPDATED),
        any(Exception.class));
  }

  @Test
  public void updateMethodAuthorizerDoesNotChangeMethodAuthorizerWhenSecurityIsEnabledAndSpecifiedClassCannotBeInstantiated() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);

    when(mockCache.isClosed()).thenThrow(new RuntimeException("Test exception"));

    doReturn(mockDistributedLockService).when(configService).getLockService(mockCache);
    when(mockDistributedLockService.lock(any(String.class), any(long.class), any(long.class)))
        .thenReturn(true);

    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    configService.updateMethodAuthorizer(mockCache, TestMethodAuthorizer.class.getName(),
        new HashSet<>());
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    verify(configService).logError(
        eq(UPDATE_ERROR_MESSAGE + INSTANTIATION_ERROR + AUTHORIZER_NOT_UPDATED),
        any(Exception.class));
  }

  @Test
  public void getLockServiceReturnsLocalLockServiceIfNotNull() {
    doReturn(mockDistributedLockService).when(configService).getExistingDistributedLockService();

    // Call getLockService() once to set the distributedLockService field in
    // QueryConfigurationServiceImpl
    DistributedLockService existingLockService = configService.getLockService(mockCache);
    assertThat(existingLockService).isSameAs(mockDistributedLockService);

    reset(configService);

    assertThat(configService.getLockService(mockCache)).isSameAs(existingLockService);

    verify(configService, times(0)).getExistingDistributedLockService();
    verify(configService, times(0)).createDistributedLockService(mockCache);
  }

  @Test
  public void getLockServiceDoesNotCreateLockServiceIfOneAlreadyExistsRemotely() {
    doReturn(mockDistributedLockService).when(configService).getExistingDistributedLockService();

    assertThat(configService.getLockService(mockCache)).isSameAs(mockDistributedLockService);

    verify(configService, times(0)).createDistributedLockService(mockCache);
  }

  @Test
  public void getLockServiceCreatesLockServiceIfNoneExists() {
    doReturn(null).when(configService).getExistingDistributedLockService();
    doReturn(mockDistributedLockService).when(configService)
        .createDistributedLockService(mockCache);

    assertThat(configService.getLockService(mockCache)).isSameAs(mockDistributedLockService);

    verify(configService).createDistributedLockService(mockCache);
  }

  @SuppressWarnings("unused")
  private Object[] getMethodAuthorizerClasses() {
    return new Object[] {
        RestrictedMethodAuthorizer.class,
        UnrestrictedMethodAuthorizer.class,
        JavaBeanAccessorMethodAuthorizer.class,
        RegExMethodAuthorizer.class
    };
  }
}
