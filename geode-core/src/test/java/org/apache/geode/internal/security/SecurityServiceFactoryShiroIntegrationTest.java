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
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_SHIRO_INIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.apache.geode.security.PostProcessor;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.util.ThreadContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.util.Properties;

@Category({IntegrationTest.class, SecurityTest.class})
public class SecurityServiceFactoryShiroIntegrationTest {

  private static final String SHIRO_INI_FILE = "SecurityServiceFactoryShiroIntegrationTest.ini";

  private String shiroIniInClasspath;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    assertThat(getClass().getResource(SHIRO_INI_FILE)).isNotNull();
    this.shiroIniInClasspath = getResourcePackage(getClass()) + SHIRO_INI_FILE;
  }

  @After
  public void after() throws Exception {
    ThreadContext.remove();
    SecurityUtils.setSecurityManager(null);
  }

  @Test
  public void getResourcePackage_shouldReturnPackageWithSlashes() throws Exception {
    String expected = "org/apache/geode/internal/security/";
    assertThat(getResourcePackage(getClass())).isEqualTo(expected);
  }

  @Test
  public void create_shiro_createsCustomSecurityService() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_SHIRO_INIT, this.shiroIniInClasspath);

    assertThat(SecurityServiceFactory.create(securityConfig, null, null))
        .isInstanceOf(CustomSecurityService.class);
  }

  @Test
  public void create_all_createsCustomSecurityService() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_SHIRO_INIT, this.shiroIniInClasspath);
    securityConfig.setProperty(SECURITY_CLIENT_AUTHENTICATOR, "value");
    securityConfig.setProperty(SECURITY_PEER_AUTHENTICATOR, "value");

    SecurityManager mockSecurityManager = mock(SecurityManager.class);
    PostProcessor mockPostProcessor = mock(PostProcessor.class);

    assertThat(
        SecurityServiceFactory.create(securityConfig, mockSecurityManager, mockPostProcessor))
            .isInstanceOf(CustomSecurityService.class);
  }

  private String getResourcePackage(Class classInPackage) {
    return classInPackage.getName().replace(classInPackage.getSimpleName(), "").replace(".", "/");
  }
}
