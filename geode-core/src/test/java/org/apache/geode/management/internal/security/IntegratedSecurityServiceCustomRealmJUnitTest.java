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

import org.apache.geode.security.TestSecurityManager;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.security.IntegratedSecurityService;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Integration tests for {@link IntegratedSecurityService} using shiro-ini.json.
 *
 * @see IntegratedSecurityServiceWithIniFileJUnitTest
 */
@Category({IntegrationTest.class, SecurityTest.class})
public class IntegratedSecurityServiceCustomRealmJUnitTest
    extends IntegratedSecurityServiceWithIniFileJUnitTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    props.put(TestSecurityManager.SECURITY_JSON,
        "org/apache/geode/management/internal/security/shiro-ini.json");
    props.put(SECURITY_MANAGER, TestSecurityManager.class.getName());
    IntegratedSecurityService.getSecurityService().initSecurity(props);
  }

}
