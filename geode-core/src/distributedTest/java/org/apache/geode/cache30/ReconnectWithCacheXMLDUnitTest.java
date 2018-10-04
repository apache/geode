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
package org.apache.geode.cache30;

import static org.apache.geode.internal.Assert.assertTrue;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.ServerLauncherParameters;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.MembershipTestHook;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.util.test.TestUtil;

/**
 * This test exercises auto-reconnect functionality when there is a cache-server that was started by
 * gfsh but was configured both by gfsh and a cache.xml file. The JIRA ticket for this is
 * GEODE-2732.
 */
@Category({MembershipTest.class, ClientServerTest.class})
public class ReconnectWithCacheXMLDUnitTest extends JUnit4CacheTestCase {
  private static final long serialVersionUID = 1L;
  private String xmlProperty = DistributionConfig.GEMFIRE_PREFIX + "autoReconnect-useCacheXMLFile";
  private String oldPropertySetting;


  public ReconnectWithCacheXMLDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() {
    oldPropertySetting = System.setProperty(xmlProperty, "true");
  }

  @Override
  public final void preTearDownCacheTestCase() {
    if (oldPropertySetting == null) {
      System.getProperties().remove(xmlProperty);
    } else {
      System.setProperty(xmlProperty, oldPropertySetting);
    }
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    String fileName = TestUtil.getResourcePath(getClass(), "ReconnectWithCacheXMLDUnitTest.xml");
    result.put(ConfigurationProperties.CACHE_XML_FILE, fileName);
    result.put(ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION, "true");
    result.put(ConfigurationProperties.DISABLE_AUTO_RECONNECT, "false");
    result.put(ConfigurationProperties.MAX_WAIT_TIME_RECONNECT, "2000");
    return result;
  }

  @Test
  public void testCacheServerLauncherPortRetained() {
    ServerLauncherParameters.INSTANCE.withPort(AvailablePortHelper.getRandomAvailableTCPPort())
        .withDisableDefaultServer(true);
    Cache cache = getCache();

    final AtomicBoolean membershipFailed = new AtomicBoolean();
    MembershipManagerHelper.addTestHook(cache.getDistributedSystem(), new MembershipTestHook() {
      @Override
      public void beforeMembershipFailure(String reason, Throwable cause) {
        membershipFailed.set(true);
      }

      @Override
      public void afterMembershipFailure(String reason, Throwable cause) {}
    });
    MembershipManagerHelper.crashDistributedSystem(cache.getDistributedSystem());
    assertTrue(membershipFailed.get());
    await()
        .until(() -> cache.getReconnectedCache() != null);

    Cache newCache = cache.getReconnectedCache();
    CacheServer server = newCache.getCacheServers().iterator().next();
    assertEquals(ServerLauncherParameters.INSTANCE.getPort().intValue(), server.getPort());
    assertEquals(20, server.getMaxConnections()); // this setting is in the XML file
  }
}
