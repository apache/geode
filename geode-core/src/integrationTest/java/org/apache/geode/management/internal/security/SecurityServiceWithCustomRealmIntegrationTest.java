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
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_SHIRO_INIT;

import org.junit.Before;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Integration tests for SecurityService using shiro-ini.json.
 *
 * @see SecurityServiceWithShiroIniIntegrationTest
 */
@Category({SecurityTest.class})
public class SecurityServiceWithCustomRealmIntegrationTest
    extends SecurityServiceWithShiroIniIntegrationTest {

  @Override
  @Before
  public void before() throws Exception {
    this.props.setProperty(TestSecurityManager.SECURITY_JSON,
        "org/apache/geode/management/internal/security/shiro-ini.json");
    this.props.setProperty(SECURITY_MANAGER, TestSecurityManager.class.getName());
    this.props.setProperty(SECURITY_SHIRO_INIT, "shiro.ini");
    this.securityService = SecurityServiceFactory.create(this.props);
  }
}
