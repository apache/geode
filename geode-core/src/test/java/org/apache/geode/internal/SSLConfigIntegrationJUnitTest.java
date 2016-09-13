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
package org.apache.geode.internal;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.management.GemFireProperties;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.assertTrue;

/**
 * Test that DistributionConfigImpl handles SSL options correctly.
 * 
 */
@Category(IntegrationTest.class)
public class SSLConfigIntegrationJUnitTest {

  @Test
  public void testIsClusterSSLRequireAuthentication() {
    Cache mCache = new CacheFactory().set(MCAST_PORT, "0").set(JMX_MANAGER, "true").create();
    ManagementService mService = ManagementService.getManagementService(mCache);
    MemberMXBean mMemberBean = mService.getMemberMXBean();
    GemFireProperties mGemFireProperties = mMemberBean.listGemFireProperties();

    assertTrue(mGemFireProperties.isServerSSLRequireAuthentication());
    assertTrue(mGemFireProperties.isClusterSSLRequireAuthentication());
    assertTrue(mGemFireProperties.isGatewaySSLRequireAuthentication());
    assertTrue(mGemFireProperties.isJmxManagerSSLRequireAuthentication());
    mCache.close();
  }
}
