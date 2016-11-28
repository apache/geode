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

package org.apache.geode.security;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.templates.DummyAuthenticator;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({IntegrationTest.class, SecurityTest.class})
public class CacheFactoryWithSecurityObjectTest {

  private SecurityService securityService;
  private SecurityManager simpleSecurityManager;
  private Properties properties = new Properties();
  Cache cache;

  @Before
  public void before() throws Exception {
    securityService = SecurityService.getSecurityService();
    simpleSecurityManager = new SimpleTestSecurityManager();
    properties.setProperty("mcast-port", "0");
  }

  @Test
  public void testCreateCacheWithSecurityManager() throws Exception {
    cache = new CacheFactory(properties).setSecurityManager(simpleSecurityManager)
        .setPostProcessor(null).create();
    assertTrue(securityService.isIntegratedSecurity());
    assertFalse(securityService.needPostProcess());
    assertNotNull(securityService.getSecurityManager());
  }

  @Test
  public void testCreateCacheWithPostProcessor() throws Exception {
    cache = new CacheFactory(properties).setPostProcessor(new TestPostProcessor())
        .setSecurityManager(null).create();
    assertFalse(securityService.isIntegratedSecurity());
    assertFalse(securityService.needPostProcess());
    assertNotNull(securityService.getPostProcessor());
  }

  @Test
  public void testOverride() throws Exception {
    properties.setProperty(ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR,
        DummyAuthenticator.class.getName());

    cache = new CacheFactory(properties).setSecurityManager(simpleSecurityManager)
        .setPostProcessor(new TestPostProcessor()).create();

    assertTrue(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertTrue(securityService.needPostProcess());
    assertNotNull(securityService.getSecurityManager());
  }

  @After
  public void after() {
    cache.close();
  }

}
