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

import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.query.security.JavaBeanAccessorMethodAuthorizer;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RegExMethodAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.cache.query.security.UnrestrictedMethodAuthorizer;
import org.apache.geode.cache.util.TestMethodAuthorizer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;

@RunWith(JUnitParamsRunner.class)
public class QueryConfigurationServiceImplTest {
  private InternalCache mockCache;
  private SecurityService mockSecurity;
  private QueryConfigurationServiceImpl configService;

  @Before
  public void setUp() {
    mockCache = mock(InternalCache.class);
    mockSecurity = mock(SecurityService.class);
    when(mockCache.getSecurityService()).thenReturn(mockSecurity);
    configService = spy(new QueryConfigurationServiceImpl());
  }

  @Test
  public void initThrowsExceptionWhenCacheIsNull() {
    Cache nullCache = null;
    assertThatThrownBy(() -> configService.init(nullCache))
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
        RestrictedMethodAuthorizer.class.getSimpleName(), new HashSet<>());

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
          RestrictedMethodAuthorizer.class.getSimpleName(), new HashSet<>());

      assertThat(configService.getMethodAuthorizer()).isSameAs(authorizer);
    } finally {
      System.clearProperty(propertyKey);
    }
  }

  @Test
  @TestCaseName("{method} Authorizer={0}")
  @Parameters(method = "getMethodAuthorizerClasses")
  public void updateMethodAuthorizerSetsCorrectAuthorizer(Class methodAuthorizerClass) {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);

    configService.updateMethodAuthorizer(mockCache, methodAuthorizerClass.getSimpleName(),
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
    verify(configService).logError(any(String.class));
  }

  @Test
  public void updateMethodAuthorizerDoesNotChangeMethodAuthorizerWhenSecurityIsEnabledAndClassNameIsNotFound() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);

    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    configService.updateMethodAuthorizer(mockCache, "FakeClassName", new HashSet<>());
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    verify(configService).logError(any(String.class));
  }

  @Test
  public void updateMethodAuthorizerDoesNotChangeMethodAuthorizerWhenSecurityIsEnabledAndSpecifiedClassHasNoValidConstructor() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);

    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    configService.updateMethodAuthorizer(mockCache, this.getClass().getName(), new HashSet<>());
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    verify(configService).logError(any(String.class));
  }

  @Test
  public void updateMethodAuthorizerDoesNotChangeMethodAuthorizerWhenSecurityIsEnabledAndSpecifiedClassCannotBeInstantiated() {
    when(mockSecurity.isIntegratedSecurity()).thenReturn(true);

    when(mockCache.isClosed()).thenThrow(new RuntimeException("Test exception"));

    configService.init(mockCache);
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    configService.updateMethodAuthorizer(mockCache, TestMethodAuthorizer.class.getName(),
        new HashSet<>());
    assertThat(configService.getMethodAuthorizer()).isInstanceOf(RestrictedMethodAuthorizer.class);
    verify(configService).logError(any(String.class));
  }

  private Object[] getMethodAuthorizerClasses() {
    return new Object[] {
        RestrictedMethodAuthorizer.class,
        UnrestrictedMethodAuthorizer.class,
        JavaBeanAccessorMethodAuthorizer.class,
        RegExMethodAuthorizer.class
    };
  }
}
