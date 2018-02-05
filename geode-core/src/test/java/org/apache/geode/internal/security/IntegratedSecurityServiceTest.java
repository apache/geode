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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.apache.shiro.session.Session;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.SubjectContext;
import org.apache.shiro.subject.support.SubjectThreadState;
import org.apache.shiro.util.ThreadContext;
import org.apache.shiro.util.ThreadState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.security.shiro.SecurityManagerProvider;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class IntegratedSecurityServiceTest {

  private SecurityManager mockSecurityManager;
  private SecurityManagerProvider provider;
  private Subject mockSubject;
  private org.apache.shiro.mgt.SecurityManager shiroManager;

  private IntegratedSecurityService securityService;

  @Before
  public void before() throws Exception {
    this.mockSecurityManager = mock(SecurityManager.class);
    this.shiroManager = mock(org.apache.shiro.mgt.SecurityManager.class);
    this.provider = mock(SecurityManagerProvider.class);
    this.mockSubject = mock(Subject.class);
    when(provider.getShiroSecurityManager()).thenReturn(shiroManager);
    when(provider.getSecurityManager()).thenReturn(mockSecurityManager);
    when(shiroManager.createSubject(any(SubjectContext.class))).thenReturn(mockSubject);
    when(mockSubject.getPrincipal()).thenReturn("principal");
    when(mockSubject.getSession()).thenReturn(mock(Session.class));

    this.securityService = new IntegratedSecurityService(provider, null);
  }

  @After
  public void after() throws Exception {
    securityService.close();
  }

  @Test
  public void bindSubject_nullSubject_shouldReturn_null() throws Exception {
    assertThatThrownBy(() -> this.securityService.bindSubject(null))
        .isInstanceOf(AuthenticationRequiredException.class)
        .hasMessageContaining("Failed to find the authenticated user");
  }

  @Test
  public void bindSubject_subject_shouldReturn_ThreadState() throws Exception {
    ThreadState threadState = this.securityService.bindSubject(this.mockSubject);
    assertThat(threadState).isNotNull().isInstanceOf(SubjectThreadState.class);
  }

  @Test
  public void login_nullProperties_shouldReturn_null() throws Exception {
    assertThatThrownBy(() -> this.securityService.login(null))
        .isInstanceOf(AuthenticationRequiredException.class)
        .hasMessageContaining("credentials are null");
  }

  @Test
  public void getSubject_login_logout() throws Exception {
    this.securityService.login(new Properties());
    Subject subject = this.securityService.getSubject();
    assertThat(subject).isNotNull();
    assertThat(ThreadContext.getSubject()).isNotNull();
    this.securityService.logout();
    assertThat(ThreadContext.getSubject()).isNull();
  }

  @Test
  public void associateWith_null_should_return_null() throws Exception {
    assertThat(this.securityService.associateWith(null)).isNull();
  }

  @Test
  public void needPostProcess_returnsFalse() throws Exception {
    boolean needPostProcess = this.securityService.needPostProcess();
    assertThat(needPostProcess).isFalse();
  }

  @Test
  public void postProcess1_value_shouldReturnSameValue() throws Exception {
    Object value = new Object();
    Object result = this.securityService.postProcess(null, null, value, false);
    assertThat(result).isNotNull().isSameAs(value);
  }

  @Test
  public void postProcess1_null_returnsNull() throws Exception {
    Object result = this.securityService.postProcess(null, null, null, false);
    assertThat(result).isNull();
  }

  @Test
  public void postProcess2_value_shouldReturnSameValue() throws Exception {
    Object value = new Object();
    Object result = this.securityService.postProcess(null, null, null, value, false);
    assertThat(result).isNotNull().isSameAs(value);
  }

  @Test
  public void postProcess2_null_returnsNull() throws Exception {
    Object result = this.securityService.postProcess(null, null, null, null, false);
    assertThat(result).isNull();
  }

  @Test
  public void isClientSecurityRequired_returnsTrue() throws Exception {
    boolean result = this.securityService.isClientSecurityRequired();
    assertThat(result).isTrue();
  }

  @Test
  public void isIntegratedSecurity_returnsTrue() throws Exception {
    boolean result = this.securityService.isIntegratedSecurity();
    assertThat(result).isTrue();
  }

  @Test
  public void isPeerSecurityRequired_returnsTrue() throws Exception {
    boolean result = this.securityService.isPeerSecurityRequired();
    assertThat(result).isTrue();
  }

  @Test
  public void getSecurityManager_returnsSecurityManager() throws Exception {
    SecurityManager securityManager = this.securityService.getSecurityManager();
    assertThat(securityManager).isNotNull().isSameAs(this.mockSecurityManager);
  }

  @Test
  public void getPostProcessor_returnsNull() throws Exception {
    PostProcessor postProcessor = this.securityService.getPostProcessor();
    assertThat(postProcessor).isNull();
  }
}
