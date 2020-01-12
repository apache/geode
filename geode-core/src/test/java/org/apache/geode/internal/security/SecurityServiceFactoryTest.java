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
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.SecurityManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.TestPostProcessor;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class SecurityServiceFactoryTest {

  private SecurityService service;
  private Properties properties;
  private org.apache.geode.security.SecurityManager securityManager;
  private PostProcessor postProcessor;
  private CacheConfig cacheConfig;

  @Before
  public void before() throws Exception {
    securityManager = mock(org.apache.geode.security.SecurityManager.class);
    postProcessor = mock(PostProcessor.class);
    cacheConfig = mock(CacheConfig.class);
    properties = new Properties();
  }

  @After
  public void after() throws Exception {
    if (service != null) {
      service.close();
    }

    // some test manually set the shiro security manager
    SecurityUtils.setSecurityManager(null);
  }

  @Test
  public void createWithNoArgument() throws Exception {
    service = SecurityServiceFactory.create();
    assertThat(service).isInstanceOf(LegacySecurityService.class);
  }

  @Test
  public void createWithPropsWithNothingOrAuthenticators() throws Exception {
    service = SecurityServiceFactory.create(properties);
    assertThat(service).isInstanceOf(LegacySecurityService.class);
    assertThat(service.isClientSecurityRequired()).isFalse();
    assertThat(service.isPeerSecurityRequired()).isFalse();

    // add client auth
    properties.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "com.abc.Auth");
    service = SecurityServiceFactory.create(properties);
    assertThat(service).isInstanceOf(LegacySecurityService.class);
    assertThat(service.isClientSecurityRequired()).isTrue();
    assertThat(service.isPeerSecurityRequired()).isFalse();

    // add peer auth
    properties.setProperty(SECURITY_PEER_AUTHENTICATOR, "com.abc.PeerAuth");
    service = SecurityServiceFactory.create(properties);
    assertThat(service).isInstanceOf(LegacySecurityService.class);
    assertThat(service.isClientSecurityRequired()).isTrue();
    assertThat(service.isPeerSecurityRequired()).isTrue();
  }

  @Test
  public void createWithPropsWithSecurityManager() throws Exception {
    properties.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    service = SecurityServiceFactory.create(properties);
    assertThat(service).isInstanceOf(IntegratedSecurityService.class);
    assertThat(service.getSecurityManager()).isNotNull();
    assertThat(service.getPostProcessor()).isNull();

    // add the post processor
    properties.setProperty(SECURITY_POST_PROCESSOR, TestPostProcessor.class.getName());
    service = SecurityServiceFactory.create(properties);
    assertThat(service).isInstanceOf(IntegratedSecurityService.class);
    assertThat(service.getSecurityManager()).isNotNull();
    assertThat(service.getPostProcessor()).isNotNull();
  }

  @Test
  public void createWithPropsWithShiro() throws Exception {
    properties.setProperty(SECURITY_SHIRO_INIT, "shiro.ini");
    service = SecurityServiceFactory.create(properties);
    assertThat(service).isInstanceOf(IntegratedSecurityService.class);
    assertThat(service.getSecurityManager()).isNull();
    assertThat(service.getPostProcessor()).isNull();
  }

  @Test
  public void shiroOverwritesSecurityManager() throws Exception {
    properties.setProperty(SECURITY_SHIRO_INIT, "shiro.ini");
    properties.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    service = SecurityServiceFactory.create(properties);
    assertThat(service).isInstanceOf(IntegratedSecurityService.class);
    assertThat(service.getSecurityManager()).isNull();
    assertThat(service.getPostProcessor()).isNull();
  }

  @Test
  public void createWithOutsideShiro() throws Exception {
    SecurityUtils.setSecurityManager(mock(SecurityManager.class));
    // create the service with empty properties, but we would still end up with
    // an IntegratedSecurityService
    service = SecurityServiceFactory.create(properties);
    assertThat(service).isInstanceOf(IntegratedSecurityService.class);
    assertThat(service.getSecurityManager()).isNull();
    assertThat(service.getPostProcessor()).isNull();
  }

  @Test
  public void cacheConfigSecurityManagerOverideShiro() throws Exception {
    properties.setProperty(SECURITY_SHIRO_INIT, "shiro.ini");
    when(cacheConfig.getSecurityManager()).thenReturn(securityManager);
    service = SecurityServiceFactory.create(properties, cacheConfig);
    assertThat(service).isInstanceOf(IntegratedSecurityService.class);
    assertThat(service.getSecurityManager()).isNotNull();
    assertThat(service.getPostProcessor()).isNull();
  }

  @Test
  public void cacheConfigOverideProperties_securityManager() throws Exception {
    properties.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    when(cacheConfig.getSecurityManager()).thenReturn(securityManager);
    service = SecurityServiceFactory.create(properties, cacheConfig);
    assertThat(service).isInstanceOf(IntegratedSecurityService.class);
    assertThat(service.getSecurityManager()).isEqualTo(securityManager);
    assertThat(service.getPostProcessor()).isNull();
  }

  @Test
  public void cacheConfigOverideProperties_postProcessor() throws Exception {
    properties.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    properties.setProperty(SECURITY_POST_PROCESSOR, TestPostProcessor.class.getName());
    when(cacheConfig.getPostProcessor()).thenReturn(postProcessor);
    service = SecurityServiceFactory.create(properties, cacheConfig);
    assertThat(service).isInstanceOf(IntegratedSecurityService.class);
    assertThat(service.getSecurityManager()).isInstanceOf(SimpleSecurityManager.class);
    assertThat(service.getPostProcessor()).isEqualTo(postProcessor);
  }

  @Test
  public void cacheConfigSecurityManagerWithPropertyPostProcessor() throws Exception {
    properties.setProperty(SECURITY_POST_PROCESSOR, TestPostProcessor.class.getName());
    when(cacheConfig.getSecurityManager()).thenReturn(securityManager);
    service = SecurityServiceFactory.create(properties, cacheConfig);
    assertThat(service).isInstanceOf(IntegratedSecurityService.class);
    assertThat(service.getSecurityManager()).isEqualTo(securityManager);
    assertThat(service.getPostProcessor()).isInstanceOf(TestPostProcessor.class);
  }

  @Test
  public void cacheConfigPostProcessorWithPropertySecurityManager() throws Exception {
    properties.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    when(cacheConfig.getPostProcessor()).thenReturn(postProcessor);
    service = SecurityServiceFactory.create(properties, cacheConfig);
    assertThat(service).isInstanceOf(IntegratedSecurityService.class);
    assertThat(service.getSecurityManager()).isInstanceOf(SimpleSecurityManager.class);
    assertThat(service.getPostProcessor()).isEqualTo(postProcessor);
  }
}
