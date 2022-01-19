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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.LegacySecurityService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.templates.DummyAuthenticator;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class CacheFactoryWithSecurityObjectTest {

  private SecurityManager simpleSecurityManager;
  private Properties properties;
  private InternalCache cache;

  @Before
  public void before() throws Exception {
    simpleSecurityManager = new SimpleSecurityManager();
    properties = new Properties();
    properties.setProperty("mcast-port", "0");
  }

  @Test
  public void testCreateCacheWithSecurityManagerOnly() throws Exception {
    cache = (InternalCache) new CacheFactory(properties)
        .setSecurityManager(simpleSecurityManager).setPostProcessor(null).create();
    SecurityService securityService = cache.getSecurityService();
    assertTrue(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertTrue(securityService.isPeerSecurityRequired());
    assertFalse(securityService.needPostProcess());
    assertNotNull(securityService.getSecurityManager());
    assertNull(securityService.getPostProcessor());
  }

  @Test
  public void testCreateCacheWithPostProcessorOnly() throws Exception {
    cache = (InternalCache) new CacheFactory(properties)
        .setPostProcessor(new TestPostProcessor()).setSecurityManager(null).create();
    SecurityService securityService = cache.getSecurityService();
    assertTrue(securityService instanceof LegacySecurityService);
    assertFalse(securityService.isIntegratedSecurity());
    assertFalse(securityService.isClientSecurityRequired());
    assertFalse(securityService.isPeerSecurityRequired());
    assertFalse(securityService.needPostProcess());
    assertNull(securityService.getSecurityManager());
    assertNull(securityService.getPostProcessor());
  }

  @Test
  public void testCreateCacheWithSecurityManagerAndPostProcessor() throws Exception {
    cache = (InternalCache) new CacheFactory(properties)
        .setSecurityManager(simpleSecurityManager).setPostProcessor(new TestPostProcessor())
        .create();
    SecurityService securityService = cache.getSecurityService();
    assertTrue(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertTrue(securityService.isPeerSecurityRequired());
    assertTrue(securityService.needPostProcess());
    assertNotNull(securityService.getSecurityManager());
    assertNotNull(securityService.getPostProcessor());
  }

  /**
   * This test seems to be misleading. Nothing is overridden here. SecurityManager is preferred over
   * SECURITY_CLIENT_AUTHENTICATOR.
   */
  @Test
  public void testSecurityManagerOverAuthenticator() throws Exception {
    properties.setProperty(ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR,
        DummyAuthenticator.class.getName());

    cache = (InternalCache) new CacheFactory(properties)
        .setSecurityManager(simpleSecurityManager).setPostProcessor(new TestPostProcessor())
        .create();

    SecurityService securityService = cache.getSecurityService();

    assertTrue(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertTrue(securityService.isPeerSecurityRequired());
    assertTrue(securityService.needPostProcess());
    assertNotNull(securityService.getSecurityManager());
    assertNotNull(securityService.getPostProcessor());
  }


  @Test
  public void testCacheConfigOverProperties1() throws Exception {
    properties.setProperty(ConfigurationProperties.SECURITY_SHIRO_INIT, "shiro.ini");

    cache = (InternalCache) new CacheFactory(properties).setSecurityManager(null)
        .setPostProcessor(null).create();

    SecurityService securityService = cache.getSecurityService();

    assertTrue(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertTrue(securityService.isPeerSecurityRequired());
    assertFalse(securityService.needPostProcess());
    assertNull(securityService.getSecurityManager());
    assertNull(securityService.getPostProcessor());
  }

  @Test
  public void testCacheConfigOverProperties() throws Exception {
    properties.setProperty(ConfigurationProperties.SECURITY_SHIRO_INIT, "shiro.ini");

    cache = (InternalCache) new CacheFactory(properties)
        .setSecurityManager(simpleSecurityManager).setPostProcessor(new TestPostProcessor())
        .create();

    SecurityService securityService = cache.getSecurityService();

    assertTrue(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertTrue(securityService.isPeerSecurityRequired());
    assertTrue(securityService.needPostProcess());
    assertNotNull(securityService.getSecurityManager());
    assertNotNull(securityService.getPostProcessor());
  }

  @Test
  public void testCacheConfigKeepsOldPostProcessor() throws Exception {
    properties.setProperty(ConfigurationProperties.SECURITY_POST_PROCESSOR,
        TestPostProcessor.class.getName());

    cache = (InternalCache) new CacheFactory(properties)
        .setSecurityManager(simpleSecurityManager).setPostProcessor(null).create();

    SecurityService securityService = cache.getSecurityService();

    assertTrue(securityService.isIntegratedSecurity());
    assertTrue(securityService.isClientSecurityRequired());
    assertTrue(securityService.isPeerSecurityRequired());
    assertTrue(securityService.needPostProcess());
    assertNotNull(securityService.getSecurityManager());
    assertNotNull(securityService.getPostProcessor());
  }


  @After
  public void after() {
    cache.close();
  }

}
