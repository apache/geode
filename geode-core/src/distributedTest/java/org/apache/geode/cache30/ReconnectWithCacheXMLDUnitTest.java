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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.internal.membership.gms.membership.GMSJoinLeave.BYPASS_DISCOVERY_PROPERTY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.ServerLauncherParameters;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MembershipTestHook;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.categories.MembershipTest;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * This test exercises auto-reconnect functionality when there is a cache-server that was started by
 * gfsh but was configured both by gfsh and a cache.xml file. The JIRA ticket for this is
 * GEODE-2732.
 */
@Category({MembershipTest.class, ClientServerTest.class})
@SuppressWarnings("serial")
public class ReconnectWithCacheXMLDUnitTest extends JUnit4CacheTestCase {

  private final String xmlProperty = GeodeGlossary.GEMFIRE_PREFIX + "autoReconnect-useCacheXMLFile";
  private String oldPropertySetting;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();
  private int locatorPort;

  @Override
  public final void postSetUp() {
    oldPropertySetting = System.setProperty(xmlProperty, "true");
    // stress testing needs this so that join attempts don't give up too soon
    Invoke.invokeInEveryVM(() -> System.setProperty("p2p.joinTimeout", "120000"));
    // reconnect tests should create their own locator so as to not impact other tests
    VM locatorVM = VM.getVM(0);
    final int port = locatorVM.invoke(() -> {
      System.setProperty(BYPASS_DISCOVERY_PROPERTY, "true");
      System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "member-weight", "100");
      return Locator.startLocatorAndDS(0, new File(""), new Properties()).getPort();
    });
    locatorPort = port;

  }

  @Override
  public final void preTearDownCacheTestCase() {
    if (oldPropertySetting == null) {
      System.getProperties().remove(xmlProperty);
    } else {
      System.setProperty(xmlProperty, oldPropertySetting);
    }
    disconnectAllFromDS();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties result = super.getDistributedSystemProperties();
    String fileName =
        createTempFileFromResource(getClass(), "ReconnectWithCacheXMLDUnitTest.xml")
            .getAbsolutePath();
    result.setProperty(ConfigurationProperties.CACHE_XML_FILE, fileName);
    result.setProperty(ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION, "true");
    result.setProperty(ConfigurationProperties.DISABLE_AUTO_RECONNECT, "false");
    result.setProperty(ConfigurationProperties.MAX_WAIT_TIME_RECONNECT, "3000");
    result.setProperty(ConfigurationProperties.MEMBER_TIMEOUT, "2000");
    result.put(LOCATORS, "localhost[" + locatorPort + "]");
    return result;
  }

  @Test
  public void usesPortSpecifiedByServerLauncherParametersWithPort() {
    ServerLauncherParameters.INSTANCE.withPort(AvailablePortHelper.getRandomAvailableTCPPort())
        .withDisableDefaultServer(true);
    Cache cache = getCache();

    AtomicBoolean membershipFailed = new AtomicBoolean();
    MembershipManagerHelper.addTestHook(cache.getDistributedSystem(), new MembershipTestHook() {
      @Override
      public void beforeMembershipFailure(String reason, Throwable cause) {
        membershipFailed.set(true);
      }
    });
    MembershipManagerHelper.crashDistributedSystem(cache.getDistributedSystem());
    await().until(membershipFailed::get);
    await()
        .until(() -> cache.getReconnectedCache() != null);

    Cache newCache = cache.getReconnectedCache();
    system = (InternalDistributedSystem) cache.getDistributedSystem();
    CacheServer server = newCache.getCacheServers().iterator().next();
    assertEquals(ServerLauncherParameters.INSTANCE.getPort().intValue(), server.getPort());
    assertEquals(20, server.getMaxConnections()); // this setting is in the XML file
  }
}
