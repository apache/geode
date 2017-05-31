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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.apache.geode.security.PostProcessor;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.subject.support.SubjectThreadState;
import org.apache.shiro.util.ThreadState;
import org.apache.geode.security.SecurityManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import java.util.concurrent.Callable;

@Category(UnitTest.class)
public class DisabledSecurityServiceTest {

  private DisabledSecurityService disabledSecurityService;
  private Subject mockSubject;

  @Before
  public void before() throws Exception {
    this.disabledSecurityService = new DisabledSecurityService();
    this.mockSubject = mock(Subject.class);
  }

  @Test
  public void bindSubject_null() throws Exception {
    ThreadState threadState = this.disabledSecurityService.bindSubject(null);
    assertThat(threadState).isNull();
  }

  @Test
  public void bindSubject_subject_shouldReturnThreadState() throws Exception {
    ThreadState threadState = this.disabledSecurityService.bindSubject(this.mockSubject);
    assertThat(threadState).isNotNull().isInstanceOf(SubjectThreadState.class);
  }

  @Test
  public void getSubject_beforeLogin_shouldReturnNull() throws Exception {
    Subject subject = this.disabledSecurityService.getSubject();
    assertThat(subject).isNull();
  }

  @Test
  public void login_null_shouldReturnNull() throws Exception {
    Subject subject = this.disabledSecurityService.login(null);
    assertThat(subject).isNull();
  }

  @Test
  public void login_properties_shouldReturnNull() throws Exception {
    Subject subject = this.disabledSecurityService.login(new Properties());
    assertThat(subject).isNull();
  }

  @Test
  public void getSubject_afterLogin_shouldReturnNull() throws Exception {
    this.disabledSecurityService.login(new Properties());
    Subject subject = this.disabledSecurityService.getSubject();
    assertThat(subject).isNull();
  }

  @Test
  public void getSubject_afterLogout_shouldReturnNull() throws Exception {
    this.disabledSecurityService.login(new Properties());
    this.disabledSecurityService.logout();
    Subject subject = this.disabledSecurityService.getSubject();
    assertThat(subject).isNull();
  }

  @Test
  public void associateWith_callable_shouldReturnSameCallable() throws Exception {
    Callable mockCallable = mock(Callable.class);
    Callable callable = this.disabledSecurityService.associateWith(mockCallable);
    assertThat(callable).isNotNull().isSameAs(mockCallable);
  }

  @Test
  public void associateWith_null_should() throws Exception {
    Callable callable = this.disabledSecurityService.associateWith(null);
    assertThat(callable).isNull();
  }

  @Test
  public void needPostProcess_returnsFalse() throws Exception {
    boolean needPostProcess = this.disabledSecurityService.needPostProcess();
    assertThat(needPostProcess).isFalse();
  }

  @Test
  public void postProcess1_value_shouldReturnSameValue() throws Exception {
    Object value = new Object();
    Object result = this.disabledSecurityService.postProcess(null, null, value, false);
    assertThat(result).isNotNull().isSameAs(value);
  }

  @Test
  public void postProcess1_null_returnsNull() throws Exception {
    Object result = this.disabledSecurityService.postProcess(null, null, null, false);
    assertThat(result).isNull();
  }

  @Test
  public void postProcess2_value_shouldReturnSameValue() throws Exception {
    Object value = new Object();
    Object result = this.disabledSecurityService.postProcess(null, null, null, value, false);
    assertThat(result).isNotNull().isSameAs(value);
  }

  @Test
  public void postProcess2_null_returnsNull() throws Exception {
    Object result = this.disabledSecurityService.postProcess(null, null, null, null, false);
    assertThat(result).isNull();
  }

  @Test
  public void isClientSecurityRequired_returnsFalse() throws Exception {
    boolean result = this.disabledSecurityService.isClientSecurityRequired();
    assertThat(result).isFalse();
  }

  @Test
  public void isIntegratedSecurity_returnsFalse() throws Exception {
    boolean result = this.disabledSecurityService.isIntegratedSecurity();
    assertThat(result).isFalse();
  }

  @Test
  public void isPeerSecurityRequired_returnsFalse() throws Exception {
    boolean result = this.disabledSecurityService.isPeerSecurityRequired();
    assertThat(result).isFalse();
  }

  @Test
  public void getSecurityManager_returnsNull() throws Exception {
    SecurityManager securityManager = this.disabledSecurityService.getSecurityManager();
    assertThat(securityManager).isNull();
  }

  @Test
  public void getPostProcessor_returnsNull() throws Exception {
    PostProcessor postProcessor = this.disabledSecurityService.getPostProcessor();
    assertThat(postProcessor).isNull();
  }
}
