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
package org.apache.geode.internal.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.apache.shiro.ShiroException;
import org.apache.shiro.UnavailableSecurityManagerException;
import org.apache.shiro.session.Session;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.SubjectContext;
import org.apache.shiro.subject.support.SubjectThreadState;
import org.apache.shiro.util.ThreadContext;
import org.apache.shiro.util.ThreadState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.security.shiro.GeodeAuthenticationToken;
import org.apache.geode.internal.security.shiro.SecurityManagerProvider;
import org.apache.geode.security.AuthenticationExpiredException;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;

public class IntegratedSecurityServiceTest {

  private SecurityManager mockSecurityManager;
  private SecurityManagerProvider provider;
  private Subject mockSubject;
  private org.apache.shiro.mgt.SecurityManager shiroManager;

  private IntegratedSecurityService securityService;
  private ShiroException shiroException;
  private Properties properties;

  @Before
  public void before() throws Exception {
    mockSecurityManager = mock(SecurityManager.class);
    shiroManager = mock(org.apache.shiro.mgt.SecurityManager.class);
    provider = mock(SecurityManagerProvider.class);
    mockSubject = mock(Subject.class);
    when(provider.getShiroSecurityManager()).thenReturn(shiroManager);
    when(provider.getSecurityManager()).thenReturn(mockSecurityManager);
    when(shiroManager.createSubject(any(SubjectContext.class))).thenReturn(mockSubject);
    when(mockSubject.getPrincipal()).thenReturn("principal");
    when(mockSubject.getSession()).thenReturn(mock(Session.class));

    shiroException = mock(ShiroException.class);
    properties = new Properties();

    securityService = new IntegratedSecurityService(provider, null);
  }

  @After
  public void after() throws Exception {
    securityService.close();
  }

  @Test
  public void bindSubject_nullSubject_shouldReturn_null() throws Exception {
    assertThatThrownBy(() -> securityService.bindSubject(null))
        .isInstanceOf(AuthenticationRequiredException.class)
        .hasMessageContaining("Failed to find the authenticated user");
  }

  @Test
  public void bindSubject_subject_shouldReturn_ThreadState() throws Exception {
    ThreadState threadState = securityService.bindSubject(mockSubject);
    assertThat(threadState).isNotNull().isInstanceOf(SubjectThreadState.class);
  }

  @Test
  public void login_nullProperties_shouldReturn_null() throws Exception {
    assertThatThrownBy(() -> securityService.login(null))
        .isInstanceOf(AuthenticationRequiredException.class)
        .hasMessageContaining("credentials are null");
  }

  @Test
  public void getSubject_login_logout() throws Exception {
    securityService.login(new Properties());
    Subject subject = securityService.getSubject();
    assertThat(subject).isNotNull();
    assertThat(ThreadContext.getSubject()).isNotNull();
    securityService.logout();
    assertThat(ThreadContext.getSubject()).isNull();
  }

  @Test
  public void associateWith_null_should_return_null() throws Exception {
    assertThat(securityService.associateWith(null)).isNull();
  }

  @Test
  public void needPostProcess_returnsFalse() throws Exception {
    boolean needPostProcess = securityService.needPostProcess();
    assertThat(needPostProcess).isFalse();
  }

  @Test
  public void postProcess1_value_shouldReturnSameValue() throws Exception {
    Object value = new Object();
    Object result = securityService.postProcess(null, null, value, false);
    assertThat(result).isNotNull().isSameAs(value);
  }

  @Test
  public void postProcess1_null_returnsNull() throws Exception {
    Object result = securityService.postProcess(null, null, null, false);
    assertThat(result).isNull();
  }

  @Test
  public void postProcess2_value_shouldReturnSameValue() throws Exception {
    Object value = new Object();
    Object result = securityService.postProcess(null, null, null, value, false);
    assertThat(result).isNotNull().isSameAs(value);
  }

  @Test
  public void postProcess2_null_returnsNull() throws Exception {
    Object result = securityService.postProcess(null, null, null, null, false);
    assertThat(result).isNull();
  }

  @Test
  public void isClientSecurityRequired_returnsTrue() throws Exception {
    boolean result = securityService.isClientSecurityRequired();
    assertThat(result).isTrue();
  }

  @Test
  public void isIntegratedSecurity_returnsTrue() throws Exception {
    boolean result = securityService.isIntegratedSecurity();
    assertThat(result).isTrue();
  }

  @Test
  public void isPeerSecurityRequired_returnsTrue() throws Exception {
    boolean result = securityService.isPeerSecurityRequired();
    assertThat(result).isTrue();
  }

  @Test
  public void getSecurityManager_returnsSecurityManager() throws Exception {
    SecurityManager securityManager = securityService.getSecurityManager();
    assertThat(securityManager).isNotNull().isSameAs(mockSecurityManager);
  }

  @Test
  public void getPostProcessor_returnsNull() throws Exception {
    PostProcessor postProcessor = securityService.getPostProcessor();
    assertThat(postProcessor).isNull();
  }

  @Test
  public void login_when_credential_isNull() throws Exception {
    assertThatThrownBy(() -> securityService.login(null))
        .isInstanceOf(AuthenticationRequiredException.class)
        .hasNoCause()
        .hasMessageContaining("credentials are null");
  }

  @Test
  public void login_when_ShiroException_hasNoCause() throws Exception {
    doThrow(shiroException).when(mockSubject).login(any(GeodeAuthenticationToken.class));
    assertThatThrownBy(() -> securityService.login(properties))
        .isInstanceOf(AuthenticationFailedException.class)
        .hasCauseInstanceOf(ShiroException.class)
        .hasMessageContaining("Authentication error. Please check your credentials");
  }

  @Test
  public void login_when_ShiroException_causedBy_AuthenticationFailed() throws Exception {
    doThrow(shiroException).when(mockSubject).login(any(GeodeAuthenticationToken.class));
    when(shiroException.getCause()).thenReturn(new AuthenticationFailedException("failed"));
    assertThatThrownBy(() -> securityService.login(properties))
        .isInstanceOf(AuthenticationFailedException.class)
        .hasNoCause()
        .hasMessageContaining("failed");
  }

  @Test
  public void login_when_ShiroException_causedBy_AuthenticationExpired() throws Exception {
    doThrow(shiroException).when(mockSubject).login(any(GeodeAuthenticationToken.class));
    when(shiroException.getCause()).thenReturn(new AuthenticationExpiredException("expired"));
    assertThatThrownBy(() -> securityService.login(properties))
        .isInstanceOf(AuthenticationExpiredException.class)
        .hasNoCause()
        .hasMessageContaining("expired");
  }

  @Test
  public void login_when_ShiroException_causedBy_OtherExceptions() throws Exception {
    doThrow(shiroException).when(mockSubject).login(any(GeodeAuthenticationToken.class));
    when(shiroException.getCause()).thenReturn(new RuntimeException("other reasons"));
    assertThatThrownBy(() -> securityService.login(properties))
        .isInstanceOf(AuthenticationFailedException.class)
        .hasCauseInstanceOf(RuntimeException.class)
        .hasMessageContaining("Authentication error. Please check your credentials.");
  }

  @Test
  public void getSubjectShouldThrowCacheClosedExceptionIfSecurityManagerUnavailable() {
    IntegratedSecurityService spy = spy(securityService);
    doThrow(new UnavailableSecurityManagerException("test")).when(spy).getCurrentUser();
    assertThatThrownBy(() -> spy.getSubject())
        .isInstanceOf(CacheClosedException.class)
        .hasMessageContaining("Cache is closed");
  }

  @Test
  public void loginShouldThrowCacheClosedExceptionIfSecurityManagerUnavailable() {
    IntegratedSecurityService spy = spy(securityService);
    doThrow(new UnavailableSecurityManagerException("test")).when(spy).getCurrentUser();
    assertThatThrownBy(() -> spy.login(new Properties()))
        .isInstanceOf(CacheClosedException.class)
        .hasMessageContaining("Cache is closed");
  }
}
