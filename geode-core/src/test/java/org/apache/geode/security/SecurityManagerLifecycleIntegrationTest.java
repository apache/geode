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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

@Category({IntegrationTest.class, SecurityTest.class})
public class SecurityManagerLifecycleIntegrationTest {

  private Properties securityProps;
  private InternalCache cache;
  private SecurityService securityService;

  @Before
  public void before() {
    this.securityProps = new Properties();
    this.securityProps.setProperty(SECURITY_MANAGER, SpySecurityManager.class.getName());

    Properties props = new Properties();
    props.putAll(this.securityProps);
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    this.cache = (InternalCache) new CacheFactory(props).create();

    this.securityService = this.cache.getSecurityService();
  }

  @After
  public void after() {
    if (this.cache != null && !this.cache.isClosed()) {
      this.cache.close();
    }
  }

  @Category(FlakyTest.class) // GEODE-1661
  @Test
  public void initAndCloseTest() {
    SpySecurityManager ssm = (SpySecurityManager) this.securityService.getSecurityManager();
    assertThat(ssm.getInitInvocationCount()).isEqualTo(1);
    this.cache.close();
    assertThat(ssm.getCloseInvocationCount()).isEqualTo(1);
  }

}
