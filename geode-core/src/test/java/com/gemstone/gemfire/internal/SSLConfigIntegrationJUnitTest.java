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
package com.gemstone.gemfire.internal;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.management.GemFireProperties;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test that DistributionConfigImpl handles SSL options correctly.
 * 
 */
@Category(IntegrationTest.class)
public class SSLConfigIntegrationJUnitTest {

  @Test
  public void test51531() {
    Cache mCache = new CacheFactory().set("mcast-port", "0").set("jmx-manager", "true").create();
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
