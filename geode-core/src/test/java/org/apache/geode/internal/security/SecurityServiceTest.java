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
  private SecurityServiceFactory securityServiceFactory;

  @Before
  public void before() {
    this.properties = new Properties();
    this.securityServiceFactory = new DefaultSecurityServiceFactory();
    this.securityService = securityServiceFactory.create();
  }

  @After
  public void after() throws Exception {
    this.securityService.close();
  }

  @Test
  public void testInitialSecurityFlags() {
    // initial state of SecurityService
    assertThat(this.securityService.isIntegratedSecurity()).isFalse();
    assertThat(this.securityService.isClientSecurityRequired()).isFalse();
    assertThat(this.securityService.isPeerSecurityRequired()).isFalse();
  }

  @Test
  public void testInitWithSecurityManager() {
    this.properties.setProperty(SECURITY_MANAGER, "org.apache.geode.security.TestSecurityManager");
    this.properties.setProperty(TestSecurityManager.SECURITY_JSON,
        "org/apache/geode/security/templates/security.json");

    this.securityService = securityServiceFactory.create(properties);

    assertThat(this.securityService.isIntegratedSecurity()).isTrue();
    assertThat(this.securityService.isClientSecurityRequired()).isTrue();
    assertThat(this.securityService.isPeerSecurityRequired()).isTrue();
  }

  @Test
  public void testInitWithClientAuthenticator() {
    this.properties.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "org.abc.test");
    this.securityService = securityServiceFactory.create(properties);

    assertThat(this.securityService.isIntegratedSecurity()).isFalse();
    assertThat(this.securityService.isClientSecurityRequired()).isTrue();
    assertThat(this.securityService.isPeerSecurityRequired()).isFalse();
  }

  @Test
  public void testInitWithPeerAuthenticator() {
    this.properties.setProperty(SECURITY_PEER_AUTHENTICATOR, "org.abc.test");
    this.securityService = securityServiceFactory.create(properties);

    assertThat(this.securityService.isIntegratedSecurity()).isFalse();
    assertThat(this.securityService.isClientSecurityRequired()).isFalse();
    assertThat(this.securityService.isPeerSecurityRequired()).isTrue();
  }

  @Test
  public void testInitWithAuthenticators() {
    this.properties.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "org.abc.test");
    this.properties.setProperty(SECURITY_PEER_AUTHENTICATOR, "org.abc.test");

    this.securityService = securityServiceFactory.create(properties);

    assertThat(this.securityService.isIntegratedSecurity()).isFalse();
    assertThat(this.securityService.isClientSecurityRequired()).isTrue();
    assertThat(this.securityService.isPeerSecurityRequired()).isTrue();
  }

  @Test
  public void testInitWithShiroAuthenticator() {
    this.properties.setProperty(SECURITY_SHIRO_INIT, "shiro.ini");

    this.securityService = securityServiceFactory.create(properties);

    assertThat(this.securityService.isIntegratedSecurity()).isTrue();
    assertThat(this.securityService.isClientSecurityRequired()).isTrue();
    assertThat(this.securityService.isPeerSecurityRequired()).isTrue();
  }

  @Test
  public void testNoInit() {
    assertThat(this.securityService.isIntegratedSecurity()).isFalse();
  }

  @Test
  public void testInitWithOutsideShiroSecurityManager() {
    SecurityUtils.setSecurityManager(new DefaultSecurityManager());
    this.securityService = securityServiceFactory.create(properties);

    assertThat(this.securityService.isIntegratedSecurity()).isTrue();
  }

}
