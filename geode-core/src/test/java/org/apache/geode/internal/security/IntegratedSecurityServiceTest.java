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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.TestPostProcessor;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category(UnitTest.class)
public class IntegratedSecurityServiceTest {

  private Properties properties;
  private SecurityService securityService;

  @Before
  public void before() {
    properties = new Properties();
    securityService = SecurityService.getSecurityService();
    securityService.initSecurity(properties);
  }

  @Test
  public void testGetObjectFromConstructor() {
    String string = SecurityService.getObjectOfType(String.class.getName(), String.class);
    assertNotNull(string);

    CharSequence charSequence =
        SecurityService.getObjectOfType(String.class.getName(), CharSequence.class);
    assertNotNull(charSequence);

    assertThatThrownBy(() -> SecurityService.getObjectOfType("com.abc.testString", String.class))
        .isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> SecurityService.getObjectOfType(String.class.getName(), Boolean.class))
        .isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> SecurityService.getObjectOfType("", String.class))
        .isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> SecurityService.getObjectOfType(null, String.class))
        .isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> SecurityService.getObjectOfType("  ", String.class))
        .isInstanceOf(GemFireSecurityException.class);
  }

  @Test
  public void testGetObjectFromFactoryMethod() {
    String string =
        SecurityService.getObjectOfType(Factories.class.getName() + ".getString", String.class);
    assertNotNull(string);

    CharSequence charSequence =
        SecurityService.getObjectOfType(Factories.class.getName() + ".getString", String.class);
    assertNotNull(charSequence);

    assertThatThrownBy(() -> SecurityService
        .getObjectOfType(Factories.class.getName() + ".getStringNonStatic", String.class))
            .isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> SecurityService
        .getObjectOfType(Factories.class.getName() + ".getNullString", String.class))
            .isInstanceOf(GemFireSecurityException.class);
  }

  @Test
  public void testInitialSecurityFlags() {
    // initial state of IntegratedSecurityService
    assertFalse(securityService.isIntegratedSecurity());
    assertFalse(securityService.isClientSecurityRequired());
    assertFalse(securityService.isPeerSecurityRequired());
  }

  @Test
  public void testInitWithSecurityManager() {
    properties.setProperty(SECURITY_MANAGER, "org.apache.geode.security.TestSecurityManager");
    properties.setProperty(TestSecurityManager.SECURITY_JSON,
        "org/apache/geode/security/templates/security.json");

    securityService.initSecurity(properties);

    assertTrue(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertTrue(securityService.isPeerSecurityRequired());
  }

  @Test
  public void testInitWithClientAuthenticator() {
    properties.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "org.abc.test");

    securityService.initSecurity(properties);
    assertFalse(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertFalse(securityService.isPeerSecurityRequired());
  }

  @Test
  public void testInitWithPeerAuthenticator() {
    properties.setProperty(SECURITY_PEER_AUTHENTICATOR, "org.abc.test");

    securityService.initSecurity(properties);

    assertFalse(securityService.isIntegratedSecurity());
    assertFalse(securityService.isClientSecurityRequired());
    assertTrue(securityService.isPeerSecurityRequired());
  }

  @Test
  public void testInitWithAuthenticators() {
    properties.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "org.abc.test");
    properties.setProperty(SECURITY_PEER_AUTHENTICATOR, "org.abc.test");

    securityService.initSecurity(properties);

    assertFalse(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertTrue(securityService.isPeerSecurityRequired());
  }

  @Test
  public void testInitWithShiroAuthenticator() {
    properties.setProperty(SECURITY_SHIRO_INIT, "shiro.ini");

    securityService.initSecurity(properties);

    assertTrue(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertTrue(securityService.isPeerSecurityRequired());
  }

  @Test
  public void testNoInit() {
    assertFalse(securityService.isIntegratedSecurity());
  }

  @Test
  public void testInitWithOutsideShiroSecurityManager() {
    SecurityUtils.setSecurityManager(new DefaultSecurityManager());
    securityService.initSecurity(properties);
    assertTrue(securityService.isIntegratedSecurity());
  }

  @Test
  public void testSetSecurityManager() {
    // initially
    assertFalse(securityService.isIntegratedSecurity());

    // init with client authenticator
    properties.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "org.abc.test");
    securityService.initSecurity(properties);
    assertFalse(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertFalse(securityService.isPeerSecurityRequired());

    // set a security manager
    securityService.setSecurityManager(new SimpleTestSecurityManager());
    assertTrue(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertTrue(securityService.isPeerSecurityRequired());
    assertFalse(securityService.needPostProcess());

    // set a post processor
    securityService.setPostProcessor(new TestPostProcessor());
    assertTrue(securityService.isIntegratedSecurity());
    assertTrue(securityService.needPostProcess());
  }

  @After
  public void after() {
    securityService.close();
  }

  private static class Factories {

    public static String getString() {
      return new String();
    }

    public static String getNullString() {
      return null;
    }

    public String getStringNonStatic() {
      return new String();
    }

    public static Boolean getBoolean() {
      return Boolean.TRUE;
    }
  }
}
