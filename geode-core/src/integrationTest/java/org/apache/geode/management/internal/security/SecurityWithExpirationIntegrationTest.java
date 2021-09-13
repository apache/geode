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
package org.apache.geode.management.internal.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.security.AuthenticationExpiredException;
import org.apache.geode.security.ExpirableSecurityManager;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({SecurityTest.class})
public class SecurityWithExpirationIntegrationTest {

  protected Properties props = new Properties();

  protected SecurityService securityService;

  @Before
  public void before() throws Exception {
    props.setProperty(SECURITY_MANAGER, ExpirableSecurityManager.class.getName());
    securityService = SecurityServiceFactory.create(this.props);
  }

  @After
  public void after() throws Exception {
    securityService.logout();
  }

  @Test
  public void testThrowAuthenticationExpiredException() {
    getSecurityManager().addExpiredUser("data");
    this.securityService.login(loginCredentials("data", "data"));
    assertThatThrownBy(() -> this.securityService.authorize(ResourcePermissions.DATA_READ))
        .isInstanceOf(AuthenticationExpiredException.class);
  }

  private Properties loginCredentials(String username, String password) {
    Properties credentials = new Properties();
    credentials.put(ResourceConstants.USER_NAME, username);
    credentials.put(ResourceConstants.PASSWORD, password);
    return credentials;
  }

  private ExpirableSecurityManager getSecurityManager() {
    return (ExpirableSecurityManager) securityService.getSecurityManager();
  }
}
