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
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_SHIRO_INIT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class SecurityServiceTest {

  private Properties properties;
  private SecurityService securityService;

  @Before
  public void before() {
    properties = new Properties();
    securityService = SecurityServiceFactory.create();
  }

  @After
  public void after() throws Exception {
    securityService.close();
  }

  @Test
  public void testInitialSecurityFlags() {
    // initial state of SecurityService
    assertThat(securityService.isIntegratedSecurity()).isFalse();
    assertThat(securityService.isClientSecurityRequired()).isFalse();
    assertThat(securityService.isPeerSecurityRequired()).isFalse();
  }

  @Test
  public void testInitWithSecurityManager() {
    properties.setProperty(SECURITY_MANAGER, "org.apache.geode.security.TestSecurityManager");
    properties.setProperty(TestSecurityManager.SECURITY_JSON,
        "org/apache/geode/security/templates/security.json");

    securityService = SecurityServiceFactory.create(properties);

    assertThat(securityService.isIntegratedSecurity()).isTrue();
    assertThat(securityService.isClientSecurityRequired()).isTrue();
    assertThat(securityService.isPeerSecurityRequired()).isTrue();
  }

  @Test
  public void testInitWithClientAuthenticator() {
    properties.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "org.abc.test");
    securityService = SecurityServiceFactory.create(properties);

    assertThat(securityService.isIntegratedSecurity()).isFalse();
    assertThat(securityService.isClientSecurityRequired()).isTrue();
    assertThat(securityService.isPeerSecurityRequired()).isFalse();
  }

  @Test
  public void testInitWithPeerAuthenticator() {
    properties.setProperty(SECURITY_PEER_AUTHENTICATOR, "org.abc.test");
    securityService = SecurityServiceFactory.create(properties);

    assertThat(securityService.isIntegratedSecurity()).isFalse();
    assertThat(securityService.isClientSecurityRequired()).isFalse();
    assertThat(securityService.isPeerSecurityRequired()).isTrue();
  }

  @Test
  public void testInitWithAuthenticators() {
    properties.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "org.abc.test");
    properties.setProperty(SECURITY_PEER_AUTHENTICATOR, "org.abc.test");

    securityService = SecurityServiceFactory.create(properties);

    assertThat(securityService.isIntegratedSecurity()).isFalse();
    assertThat(securityService.isClientSecurityRequired()).isTrue();
    assertThat(securityService.isPeerSecurityRequired()).isTrue();
  }

  @Test
  public void testInitWithShiroAuthenticator() {
    properties.setProperty(SECURITY_SHIRO_INIT, "shiro.ini");

    securityService = SecurityServiceFactory.create(properties);

    assertThat(securityService.isIntegratedSecurity()).isTrue();
    assertThat(securityService.isClientSecurityRequired()).isTrue();
    assertThat(securityService.isPeerSecurityRequired()).isTrue();
  }

  @Test
  public void testNoInit() {
    assertThat(securityService.isIntegratedSecurity()).isFalse();
  }

  @Test
  public void testInitWithOutsideShiroSecurityManager() {
    SecurityUtils.setSecurityManager(new DefaultSecurityManager());
    securityService = SecurityServiceFactory.create(properties);

    assertThat(securityService.isIntegratedSecurity()).isTrue();
  }

}
