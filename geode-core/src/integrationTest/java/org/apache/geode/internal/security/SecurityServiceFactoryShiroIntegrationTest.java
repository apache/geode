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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_SHIRO_INIT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class SecurityServiceFactoryShiroIntegrationTest {

  private static final String SHIRO_INI_FILE = "SecurityServiceFactoryShiroIntegrationTest.ini";

  private String shiroIniInClasspath;

  private SecurityService service;


  @Before
  public void before() throws Exception {
    assertThat(getClass().getResource(SHIRO_INI_FILE)).isNotNull();
    shiroIniInClasspath = getResourcePackage(getClass()) + SHIRO_INI_FILE;
  }

  @After
  public void after() throws Exception {
    if (service != null) {
      service.close();
    }
  }

  @Test
  public void getResourcePackage_shouldReturnPackageWithSlashes() throws Exception {
    String expected = "org/apache/geode/internal/security/";
    assertThat(getResourcePackage(getClass())).isEqualTo(expected);
  }

  @Test
  public void create_shiro_createsCustomSecurityService() throws Exception {
    Properties securityConfig = new Properties();
    securityConfig.setProperty(SECURITY_SHIRO_INIT, shiroIniInClasspath);
    service = SecurityServiceFactory.create(securityConfig);
    assertThat(service).isInstanceOf(IntegratedSecurityService.class);
  }

  private String getResourcePackage(Class classInPackage) {
    return classInPackage.getName().replace(classInPackage.getSimpleName(), "").replace(".", "/");
  }
}
