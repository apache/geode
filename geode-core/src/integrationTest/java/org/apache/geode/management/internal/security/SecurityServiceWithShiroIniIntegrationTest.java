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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_SHIRO_INIT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.security.DefaultSecurityServiceFactory;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Integration tests for SecurityService using shiro.ini
 */
@Category({SecurityTest.class})
public class SecurityServiceWithShiroIniIntegrationTest {

  protected Properties props = new Properties();

  protected SecurityService securityService;
  protected SecurityServiceFactory securityServiceFactory;

  @Before
  public void before() throws Exception {
    this.props.setProperty(SECURITY_SHIRO_INIT, "shiro.ini");
    this.securityServiceFactory = new DefaultSecurityServiceFactory();
    this.securityService = securityServiceFactory.create(this.props);
  }

  @After
  public void after() throws Exception {
    this.securityService.logout();
  }

  @Test
  public void testRoot() throws Exception {
    this.securityService.login(loginCredentials("root", "secret"));
    this.securityService.authorize(TestCommand.none);
    this.securityService.authorize(TestCommand.everyOneAllowed);
    this.securityService.authorize(ResourcePermissions.DATA_READ);
    this.securityService.authorize(ResourcePermissions.DATA_WRITE);
    this.securityService.authorize(TestCommand.regionARead);
    this.securityService.authorize(TestCommand.regionAWrite);
    this.securityService.authorize(ResourcePermissions.CLUSTER_WRITE);
    this.securityService.authorize(ResourcePermissions.CLUSTER_READ);
  }

  @Test
  public void testGuest() throws Exception {
    this.securityService.login(loginCredentials("guest", "guest"));
    this.securityService.authorize(TestCommand.none);
    this.securityService.authorize(TestCommand.everyOneAllowed);

    assertNotAuthorized(ResourcePermissions.DATA_READ);
    assertNotAuthorized(ResourcePermissions.DATA_WRITE);
    assertNotAuthorized(TestCommand.regionARead);
    assertNotAuthorized(TestCommand.regionAWrite);
    assertNotAuthorized(ResourcePermissions.CLUSTER_READ);
    assertNotAuthorized(ResourcePermissions.CLUSTER_WRITE);
  }

  @Test
  public void testRegionAReader() throws Exception {
    this.securityService.login(loginCredentials("regionAReader", "password"));
    this.securityService.authorize(TestCommand.none);
    this.securityService.authorize(TestCommand.everyOneAllowed);
    this.securityService.authorize(TestCommand.regionARead);

    assertNotAuthorized(TestCommand.regionAWrite);
    assertNotAuthorized(ResourcePermissions.DATA_READ);
    assertNotAuthorized(ResourcePermissions.DATA_WRITE);
    assertNotAuthorized(ResourcePermissions.CLUSTER_READ);
    assertNotAuthorized(ResourcePermissions.CLUSTER_WRITE);
  }

  @Test
  public void testRegionAUser() throws Exception {
    this.securityService.login(loginCredentials("regionAUser", "password"));
    this.securityService.authorize(TestCommand.none);
    this.securityService.authorize(TestCommand.everyOneAllowed);
    this.securityService.authorize(TestCommand.regionAWrite);
    this.securityService.authorize(TestCommand.regionARead);

    assertNotAuthorized(ResourcePermissions.DATA_READ);
    assertNotAuthorized(ResourcePermissions.DATA_WRITE);
    assertNotAuthorized(ResourcePermissions.CLUSTER_READ);
    assertNotAuthorized(ResourcePermissions.CLUSTER_WRITE);
  }

  @Test
  public void testDataReader() throws Exception {
    this.securityService.login(loginCredentials("dataReader", "12345"));
    this.securityService.authorize(TestCommand.none);
    this.securityService.authorize(TestCommand.everyOneAllowed);
    this.securityService.authorize(TestCommand.regionARead);
    this.securityService.authorize(ResourcePermissions.DATA_READ);

    assertNotAuthorized(TestCommand.regionAWrite);
    assertNotAuthorized(ResourcePermissions.DATA_WRITE);
    assertNotAuthorized(ResourcePermissions.CLUSTER_READ);
    assertNotAuthorized(ResourcePermissions.CLUSTER_WRITE);
  }

  @Test
  public void testReader() throws Exception {
    this.securityService.login(loginCredentials("reader", "12345"));
    this.securityService.authorize(TestCommand.none);
    this.securityService.authorize(TestCommand.everyOneAllowed);
    this.securityService.authorize(TestCommand.regionARead);
    this.securityService.authorize(ResourcePermissions.DATA_READ);
    this.securityService.authorize(ResourcePermissions.CLUSTER_READ);

    assertNotAuthorized(TestCommand.regionAWrite);
    assertNotAuthorized(ResourcePermissions.DATA_WRITE);
    assertNotAuthorized(ResourcePermissions.CLUSTER_WRITE);
  }

  private void assertNotAuthorized(ResourcePermission context) {
    assertThatThrownBy(() -> this.securityService.authorize(context))
        .isInstanceOf(GemFireSecurityException.class).hasMessageContaining(context.toString());
  }

  private Properties loginCredentials(String username, String password) {
    Properties credentials = new Properties();
    credentials.put(ResourceConstants.USER_NAME, username);
    credentials.put(ResourceConstants.PASSWORD, password);
    return credentials;
  }
}
