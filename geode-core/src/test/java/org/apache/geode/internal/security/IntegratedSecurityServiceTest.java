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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Java6Assertions.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.geode.security.templates.SampleSecurityManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.test.junit.categories.UnitTest;

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
    properties.setProperty(SECURITY_MANAGER,
        "org.apache.geode.security.templates.SampleSecurityManager");
    properties.setProperty(SampleSecurityManager.SECURITY_JSON,
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
  public void testInitWithShiroAuthenticator() {
    properties.setProperty(SECURITY_SHIRO_INIT, "shiro.ini");

    securityService.initSecurity(properties);

    assertTrue(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertTrue(securityService.isPeerSecurityRequired());
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
