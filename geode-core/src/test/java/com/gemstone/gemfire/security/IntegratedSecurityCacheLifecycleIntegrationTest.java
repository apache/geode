/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.security;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Assertions.*;

import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.security.GeodeSecurityUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class, SecurityTest.class})
public class IntegratedSecurityCacheLifecycleIntegrationTest {

  private Properties securityProps;
  private Cache cache;

  @Before
  public void before() {
    securityProps = new Properties();
    securityProps.setProperty(SECURITY_MANAGER, SpySecurityManager.class.getName());

    Properties props = new Properties();
    props.putAll(securityProps);
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    cache = new CacheFactory(props).create();
  }

  @After
  public void after() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
  }

  @Test
  public void initAndCloseTest () {
    SpySecurityManager ssm = (SpySecurityManager)GeodeSecurityUtil.getSecurityManager();
    assertThat(ssm.initInvoked).isEqualTo(1);
    cache.close();
    assertThat(ssm.closeInvoked).isEqualTo(1);
  }

}
