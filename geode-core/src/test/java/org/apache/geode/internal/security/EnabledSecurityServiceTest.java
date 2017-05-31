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
import static org.mockito.Mockito.*;

import org.apache.geode.internal.security.shiro.RealmInitializer;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.support.SubjectThreadState;
import org.apache.shiro.util.ThreadState;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import java.util.concurrent.Callable;

@Category(UnitTest.class)
public class EnabledSecurityServiceTest {

  private SecurityManager mockSecurityManager;
  private PostProcessor mockPostProcessor;
  private RealmInitializer spyRealmInitializer;
  private Subject mockSubject;

  private EnabledSecurityService securityService;
  private EnabledSecurityService securityServiceWithPostProcessor;

  @Before
  public void before() throws Exception {
    this.mockSecurityManager = mock(SecurityManager.class);
    this.mockPostProcessor = mock(PostProcessor.class);
    this.spyRealmInitializer = spy(RealmInitializer.class);
    this.mockSubject = mock(Subject.class);

    this.securityService =
        new EnabledSecurityService(this.mockSecurityManager, null, this.spyRealmInitializer);
    this.securityServiceWithPostProcessor = new EnabledSecurityService(this.mockSecurityManager,
        this.mockPostProcessor, this.spyRealmInitializer);
  }

  @Test
  public void bindSubject_nullSubject_shouldReturn_null() throws Exception {
    ThreadState threadState = this.securityService.bindSubject(null);
    assertThat(threadState).isNull();
  }

  @Test
  public void bindSubject_subject_shouldReturn_ThreadState() throws Exception {
    ThreadState threadState = this.securityService.bindSubject(this.mockSubject);
    assertThat(threadState).isNotNull().isInstanceOf(SubjectThreadState.class);
  }

  @Test
  public void getSubject_beforeLogin_shouldThrow_GemFireSecurityException() throws Exception {
    assertThatThrownBy(() -> this.securityService.getSubject())
        .isInstanceOf(GemFireSecurityException.class).hasMessageContaining("Anonymous User");
  }

  @Test
  public void login_nullProperties_shouldReturn_null() throws Exception {
    Subject subject = this.securityService.login(null);
    assertThat(subject).isNull();
  }

  @Test
  public void login_emptyProperties_shouldThrow_AuthenticationFailedException() throws Exception {
    assertThatThrownBy(() -> this.securityService.login(new Properties()))
        .isInstanceOf(AuthenticationFailedException.class)
        .hasMessageContaining("Please check your credentials");
  }

  @Ignore("Extract all shiro integration code out of EnabledSecurityService for mocking")
  @Test
  public void getSubject_afterLogin_shouldReturnNull() throws Exception {
    this.securityService.login(new Properties());
    Subject subject = this.securityService.getSubject();
    assertThat(subject).isNull();
  }

  @Ignore("Extract all shiro integration code out of EnabledSecurityService for mocking")
  @Test
  public void getSubject_afterLogout_shouldReturnNull() throws Exception {
    this.securityService.login(new Properties());
    this.securityService.logout();
    Subject subject = this.securityService.getSubject();
    assertThat(subject).isNull();
  }

  @Test
  public void associateWith_callable_beforeLogin_shouldThrow_GemFireSecurityException()
      throws Exception {
    assertThatThrownBy(() -> this.securityService.associateWith(mock(Callable.class)))
        .isInstanceOf(GemFireSecurityException.class).hasMessageContaining("Anonymous User");
  }

  @Test
  public void associateWith_null_should() throws Exception {
    assertThatThrownBy(() -> this.securityService.associateWith(null))
        .isInstanceOf(GemFireSecurityException.class).hasMessageContaining("Anonymous User");
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
