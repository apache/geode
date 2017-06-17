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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.shiro.SecurityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.security.shiro.SecurityManagerProvider;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class IntegratedSecurityServiceConstructorTest {

  private IntegratedSecurityService securityService;
  private SecurityManager securityManager;
  private PostProcessor postProcessor;
  private SecurityManagerProvider provider;
  private org.apache.shiro.mgt.SecurityManager shiroManager;

  @Before
  public void before() throws Exception {
    securityManager = mock(SecurityManager.class);
    postProcessor = mock(PostProcessor.class);
    provider = mock(SecurityManagerProvider.class);
    shiroManager = mock(org.apache.shiro.mgt.SecurityManager.class);
    when(provider.getShiroSecurityManager()).thenReturn(shiroManager);
  }

  @After
  public void after() throws Exception {
    if (securityService != null) {
      securityService.close();
    }

    // some test manually set the shiro security manager
    SecurityUtils.setSecurityManager(null);
  }

  @Test
  public void constructorWithOutsideShrio() throws Exception {
    when(provider.getSecurityManager()).thenReturn(null);
    securityService = new IntegratedSecurityService(provider, postProcessor);
    assertThat(securityService.getPostProcessor()).isEqualTo(postProcessor);
    assertThat(securityService.getSecurityManager()).isNull();
    assertIntegratedSecurityService();
  }

  @Test
  public void constructorWithSecurityManager() throws Exception {
    when(provider.getSecurityManager()).thenReturn(securityManager);
    securityService = new IntegratedSecurityService(provider, null);
    assertThat(securityService.getPostProcessor()).isNull();
    assertThat(securityService.getSecurityManager()).isEqualTo(securityManager);
    assertIntegratedSecurityService();
  }

  @Test
  public void constructorWithSecurityManagerAndPostProcessor() throws Exception {
    when(provider.getSecurityManager()).thenReturn(securityManager);
    securityService = new IntegratedSecurityService(provider, postProcessor);
    assertThat(securityService.getPostProcessor()).isEqualTo(postProcessor);
    assertThat(securityService.getSecurityManager()).isEqualTo(securityManager);
    assertIntegratedSecurityService();
  }

  private void assertIntegratedSecurityService() throws Exception {
    assertThat(securityService.isIntegratedSecurity()).isTrue();
    assertThat(securityService.isClientSecurityRequired()).isTrue();
    assertThat(securityService.isPeerSecurityRequired()).isTrue();
  }
}
