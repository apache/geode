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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_POST_PROCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_SHIRO_INIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.util.ThreadContext;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({UnitTest.class, SecurityTest.class})
public class SecurityServiceFactoryTest {

  @After
  public void after() throws Exception {
    ThreadContext.remove();
    SecurityUtils.setSecurityManager(null);
  }

  @Test
  public void getPostProcessor_null_returnsNull() throws Exception {
    assertThat(SecurityServiceFactory.getPostProcessor(null, null)).isNull();
  }

  @Test
  public void getPostProcessor_returnsPostProcessor() throws Exception {
    PostProcessor mockPostProcessor = mock(PostProcessor.class);

    assertThat(SecurityServiceFactory.getPostProcessor(mockPostProcessor, null))
        .isSameAs(mockPostProcessor);
  }

  @Test
  public void getPostProcessor_SecurityConfig_createsPostProcessor() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_POST_PROCESSOR, FakePostProcessor.class.getName());

    PostProcessor postProcessor = SecurityServiceFactory.getPostProcessor(null, securityConfig);

    assertThat(postProcessor).isInstanceOf(FakePostProcessor.class);

    FakePostProcessor fakePostProcessor = (FakePostProcessor) postProcessor;

    assertThat(fakePostProcessor.getInitInvocations()).isEqualTo(0);
    assertThat(fakePostProcessor.getSecurityProps()).isNull();
  }

  @Test
  public void getPostProcessor_prefersPostProcessorOverSecurityConfig() throws Exception {
    PostProcessor mockPostProcessor = mock(PostProcessor.class);
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_POST_PROCESSOR, FakePostProcessor.class.getName());

    assertThat(SecurityServiceFactory.getPostProcessor(mockPostProcessor, securityConfig))
        .isSameAs(mockPostProcessor);
  }

  @Test
  public void getSecurityManager_null_returnsNull() throws Exception {
    assertThat(SecurityServiceFactory.getSecurityManager(null, null)).isNull();
  }

  @Test
  public void getSecurityManager_returnsSecurityManager() throws Exception {
    SecurityManager mockSecurityManager = mock(SecurityManager.class);

    assertThat(SecurityServiceFactory.getSecurityManager(mockSecurityManager, null))
        .isSameAs(mockSecurityManager);
  }

  @Test
  public void getSecurityManager_SecurityConfig_createsSecurityManager() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_MANAGER, FakeSecurityManager.class.getName());

    SecurityManager securityManager =
        SecurityServiceFactory.getSecurityManager(null, securityConfig);

    assertThat(securityManager).isInstanceOf(FakeSecurityManager.class);

    FakeSecurityManager fakeSecurityManager = (FakeSecurityManager) securityManager;

    assertThat(fakeSecurityManager.getInitInvocations()).isEqualTo(0);
    assertThat(fakeSecurityManager.getSecurityProps()).isNull();
  }

  @Test
  public void getSecurityManager_prefersSecurityManagerOverSecurityConfig() throws Exception {
    SecurityManager mockSecurityManager = mock(SecurityManager.class);
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_MANAGER, FakePostProcessor.class.getName());

    assertThat(SecurityServiceFactory.getSecurityManager(mockSecurityManager, securityConfig))
        .isSameAs(mockSecurityManager);
  }

  @Test
  public void determineType_null_returnsDISABLED() throws Exception {
    assertThat(SecurityServiceFactory.determineType(null, null, null))
        .isSameAs(SecurityServiceType.DISABLED);
  }

  @Test
  public void determineType_shiro_returnsCUSTOM() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_SHIRO_INIT, "value");

    assertThat(SecurityServiceFactory.determineType(securityConfig, null, null))
        .isSameAs(SecurityServiceType.CUSTOM);
  }

  @Test
  public void determineType_securityManager_returnsENABLED() throws Exception {
    Properties securityConfig = new Properties();
    SecurityManager mockSecurityManager = mock(SecurityManager.class);

    assertThat(SecurityServiceFactory.determineType(securityConfig, mockSecurityManager, null))
        .isSameAs(SecurityServiceType.ENABLED);
  }

  @Test
  public void determineType_postProcessor_returnsDISABLED() throws Exception {
    Properties securityConfig = new Properties();
    PostProcessor mockPostProcessor = mock(PostProcessor.class);

    assertThat(SecurityServiceFactory.determineType(securityConfig, null, mockPostProcessor))
        .isSameAs(SecurityServiceType.DISABLED);
  }

  @Test
  public void determineType_both_returnsENABLED() throws Exception {
    Properties securityConfig = new Properties();
    SecurityManager mockSecurityManager = mock(SecurityManager.class);
    PostProcessor mockPostProcessor = mock(PostProcessor.class);

    assertThat(SecurityServiceFactory.determineType(securityConfig, mockSecurityManager,
        mockPostProcessor)).isSameAs(SecurityServiceType.ENABLED);
  }

  @Test
  public void determineType_prefersCUSTOM() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_SHIRO_INIT, "value");
    securityConfig.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "value");
    securityConfig.setProperty(SECURITY_PEER_AUTHENTICATOR, "value");
    SecurityManager mockSecurityManager = mock(SecurityManager.class);

    assertThat(SecurityServiceFactory.determineType(securityConfig, mockSecurityManager, null))
        .isSameAs(SecurityServiceType.CUSTOM);
  }

  @Test
  public void determineType_clientAuthenticator_returnsLEGACY() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "value");

    assertThat(SecurityServiceFactory.determineType(securityConfig, null, null))
        .isSameAs(SecurityServiceType.LEGACY);
  }

  @Test
  public void determineType_peerAuthenticator_returnsLEGACY() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_PEER_AUTHENTICATOR, "value");

    assertThat(SecurityServiceFactory.determineType(securityConfig, null, null))
        .isSameAs(SecurityServiceType.LEGACY);
  }

  @Test
  public void determineType_authenticators_returnsLEGACY() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "value");
    securityConfig.setProperty(SECURITY_PEER_AUTHENTICATOR, "value");

    assertThat(SecurityServiceFactory.determineType(securityConfig, null, null))
        .isSameAs(SecurityServiceType.LEGACY);
  }

  @Test
  public void determineType_empty_returnsDISABLED() throws Exception {
    Properties securityConfig = new Properties();

    assertThat(SecurityServiceFactory.determineType(securityConfig, null, null))
        .isSameAs(SecurityServiceType.DISABLED);
  }

  @Test
  public void create_clientAuthenticator_createsLegacySecurityService() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "value");

    assertThat(SecurityServiceFactory.create(securityConfig, null, null))
        .isInstanceOf(LegacySecurityService.class);
  }

  @Test
  public void create_peerAuthenticator_createsLegacySecurityService() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_PEER_AUTHENTICATOR, "value");

    assertThat(SecurityServiceFactory.create(securityConfig, null, null))
        .isInstanceOf(LegacySecurityService.class);
  }

  @Test
  public void create_authenticators_createsLegacySecurityService() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "value");
    securityConfig.setProperty(SECURITY_PEER_AUTHENTICATOR, "value");

    assertThat(SecurityServiceFactory.create(securityConfig, null, null))
        .isInstanceOf(LegacySecurityService.class);
  }

  @Test
  public void create_none_createsDisabledSecurityService() throws Exception {
    Properties securityConfig = new Properties();

    assertThat(SecurityServiceFactory.create(securityConfig, null, null))
        .isInstanceOf(DisabledSecurityService.class);
  }

  @Test
  public void create_postProcessor_createsDisabledSecurityService() throws Exception {
    Properties securityConfig = new Properties();
    PostProcessor mockPostProcessor = mock(PostProcessor.class);

    assertThat(SecurityServiceFactory.create(securityConfig, null, mockPostProcessor))
        .isInstanceOf(DisabledSecurityService.class);
  }

  @Test
  public void create_securityManager_createsEnabledSecurityService() throws Exception {
    Properties securityConfig = new Properties();
    SecurityManager mockSecurityManager = mock(SecurityManager.class);

    assertThat(SecurityServiceFactory.create(securityConfig, mockSecurityManager, null))
        .isInstanceOf(EnabledSecurityService.class);
  }

  @Test
  public void create_securityManagerAndPostProcessor_createsEnabledSecurityService()
      throws Exception {
    Properties securityConfig = new Properties();
    SecurityManager mockSecurityManager = mock(SecurityManager.class);
    PostProcessor mockPostProcessor = mock(PostProcessor.class);

    assertThat(
        SecurityServiceFactory.create(securityConfig, mockSecurityManager, mockPostProcessor))
            .isInstanceOf(EnabledSecurityService.class);
  }

}
