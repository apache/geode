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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Properties;

import org.apache.geode.security.ResourcePermission;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.security.IntegratedSecurityService;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Integration tests for {@link IntegratedSecurityService} using shiro.ini
 */
@Category({IntegrationTest.class, SecurityTest.class})
public class IntegratedSecurityServiceWithIniFileJUnitTest {

  protected static Properties props = new Properties();

  private SecurityService securityService = SecurityService.getSecurityService();

  @BeforeClass
  public static void beforeClass() throws Exception {
    props.setProperty(SECURITY_SHIRO_INIT, "shiro.ini");
  }

  @Before
  public void before() {
    securityService.initSecurity(props);
  }

  @Test
  public void testRoot() {
    this.securityService.login("root", "secret");
    this.securityService.authorize(TestCommand.none);
    this.securityService.authorize(TestCommand.everyOneAllowed);
    this.securityService.authorize(TestCommand.dataRead);
    this.securityService.authorize(TestCommand.dataWrite);
    this.securityService.authorize(TestCommand.regionARead);
    this.securityService.authorize(TestCommand.regionAWrite);
    this.securityService.authorize(TestCommand.clusterWrite);
    this.securityService.authorize(TestCommand.clusterRead);
  }

  @Test
  public void testGuest() {
    this.securityService.login("guest", "guest");
    this.securityService.authorize(TestCommand.none);
    this.securityService.authorize(TestCommand.everyOneAllowed);

    assertNotAuthorized(TestCommand.dataRead);
    assertNotAuthorized(TestCommand.dataWrite);
    assertNotAuthorized(TestCommand.regionARead);
    assertNotAuthorized(TestCommand.regionAWrite);
    assertNotAuthorized(TestCommand.clusterRead);
    assertNotAuthorized(TestCommand.clusterWrite);
    this.securityService.logout();
  }

  @Test
  public void testRegionAReader() {
    this.securityService.login("regionAReader", "password");
    this.securityService.authorize(TestCommand.none);
    this.securityService.authorize(TestCommand.everyOneAllowed);
    this.securityService.authorize(TestCommand.regionARead);

    assertNotAuthorized(TestCommand.regionAWrite);
    assertNotAuthorized(TestCommand.dataRead);
    assertNotAuthorized(TestCommand.dataWrite);
    assertNotAuthorized(TestCommand.clusterRead);
    assertNotAuthorized(TestCommand.clusterWrite);
    this.securityService.logout();
  }

  @Test
  public void testRegionAUser() {
    this.securityService.login("regionAUser", "password");
    this.securityService.authorize(TestCommand.none);
    this.securityService.authorize(TestCommand.everyOneAllowed);
    this.securityService.authorize(TestCommand.regionAWrite);
    this.securityService.authorize(TestCommand.regionARead);

    assertNotAuthorized(TestCommand.dataRead);
    assertNotAuthorized(TestCommand.dataWrite);
    assertNotAuthorized(TestCommand.clusterRead);
    assertNotAuthorized(TestCommand.clusterWrite);
    this.securityService.logout();
  }

  @Test
  public void testDataReader() {
    this.securityService.login("dataReader", "12345");
    this.securityService.authorize(TestCommand.none);
    this.securityService.authorize(TestCommand.everyOneAllowed);
    this.securityService.authorize(TestCommand.regionARead);
    this.securityService.authorize(TestCommand.dataRead);

    assertNotAuthorized(TestCommand.regionAWrite);
    assertNotAuthorized(TestCommand.dataWrite);
    assertNotAuthorized(TestCommand.clusterRead);
    assertNotAuthorized(TestCommand.clusterWrite);
    this.securityService.logout();
  }

  @Test
  public void testReader() {
    this.securityService.login("reader", "12345");
    this.securityService.authorize(TestCommand.none);
    this.securityService.authorize(TestCommand.everyOneAllowed);
    this.securityService.authorize(TestCommand.regionARead);
    this.securityService.authorize(TestCommand.dataRead);
    this.securityService.authorize(TestCommand.clusterRead);

    assertNotAuthorized(TestCommand.regionAWrite);
    assertNotAuthorized(TestCommand.dataWrite);
    assertNotAuthorized(TestCommand.clusterWrite);
    this.securityService.logout();
  }

  private void assertNotAuthorized(ResourcePermission context) {
    assertThatThrownBy(() -> this.securityService.authorize(context))
        .isInstanceOf(GemFireSecurityException.class).hasMessageContaining(context.toString());
  }
}
