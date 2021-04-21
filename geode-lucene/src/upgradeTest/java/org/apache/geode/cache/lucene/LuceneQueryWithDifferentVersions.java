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
package org.apache.geode.cache.lucene;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;

public class LuceneQueryWithDifferentVersions
    extends LuceneSearchWithRollingUpgradeDUnit {

  // 2 locator, 2 servers
  @Test
  public void luceneQueryCannotBeExecuted()
      throws Exception {
    final Host host = Host.getHost(0);
    VM locator1 = host.getVM(oldVersion, 0);
    VM locator2 = host.getVM(oldVersion, 1);
    VM server1 = host.getVM(oldVersion, 2);
    VM server2 = host.getVM(oldVersion, 3);

    final String regionName = "aRegion";
    RegionShortcut shortcut = RegionShortcut.PARTITION_REDUNDANT;
    String regionType = "partitionedRedundant";

    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    locator1.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(locatorPorts));
    locator2.invoke(() -> DistributedTestUtils.deleteLocatorStateFile(locatorPorts));

    String hostName = NetworkUtils.getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);
    try {
      locator1.invoke(
          invokeStartLocator(hostName, locatorPorts[0], getLocatorPropertiesPre91(locatorString)));
      locator2.invoke(
          invokeStartLocator(hostName, locatorPorts[1], getLocatorPropertiesPre91(locatorString)));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server1, server2);

      // Locators before 1.4 handled configuration asynchronously.
      // We must wait for configuration configuration to be ready, or confirm that it is disabled.
      locator1.invoke(
          () -> await().untilAsserted(() -> await().untilAsserted(() -> {
            assertThat(InternalLocator.getLocator().isSharedConfigurationRunning()).isTrue();
          })));
      locator2.invoke(
          () -> await()
              .untilAsserted(() -> await().untilAsserted(() -> {
                assertThat(InternalLocator.getLocator().isSharedConfigurationRunning()).isTrue();
              })));

      server1.invoke(() -> createLuceneIndex(cache, regionName, INDEX_NAME));
      server2.invoke(() -> createLuceneIndex(cache, regionName, INDEX_NAME));

      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcut.name()), server1, server2);

      putSerializableObjectAndVerifyLuceneQueryResult(server1, regionName, 10, 0,
          10, server1, server2);
      locator1 = rollLocatorToCurrent(locator1, hostName, locatorPorts[0], getTestMethodName(),
          locatorString);

      locator2 = rollLocatorToCurrent(locator2, hostName, locatorPorts[1], getTestMethodName(),
          locatorString);

      server1 = rollServerToCurrentCreateLuceneIndexAndCreateRegion(server1, regionType, null,
          shortcut.name(), regionName, locatorPorts, reindex);

      Throwable thrown = catchThrowable(() -> {
        putSerializableObjectAndVerifyLuceneQueryResult(server2, regionName, 20, 15,
            25, server2);
      });

      assertThat(thrown.getCause()).isInstanceOf(AssertionError.class);

    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator1, locator2);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2);
    }
  }
}
