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
package org.apache.geode.management.internal;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.management.internal.SystemManagementService.FEDERATING_MANAGER_FACTORY_PROPERTY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import javax.management.ObjectName;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.stubbing.Answer;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedErrorCollector;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.JMXTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category(JMXTest.class)
public class MBeanFederationErrorHandlingDistributedTest implements Serializable {

  private static final String REGION_NAME = "test-region-1";

  private static LocatorLauncher locatorLauncher;
  private static ServerLauncher serverLauncher;
  private static MBeanProxyFactory proxyFactory;

  private ObjectName regionMXBeanName;
  private String locatorName;
  private String serverName;
  private int locatorPort;
  private VM locatorVM;
  private VM serverVM;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public DistributedErrorCollector errorCollector = new DistributedErrorCollector();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() throws Exception {
    locatorName = "locator";
    serverName = "server";
    regionMXBeanName =
        new ObjectName(String.format("GemFire:service=Region,name=\"%s\",type=Member,member=%s",
            SEPARATOR + REGION_NAME, serverName));

    locatorVM = getController();
    serverVM = getVM(0);

    locatorPort = locatorVM.invoke(this::startLocator);

    serverVM.invoke(this::startServer);
  }

  @After
  public void tearDown() {
    locatorVM.invoke(() -> {
      if (locatorLauncher != null) {
        locatorLauncher.stop();
        locatorLauncher = null;
        proxyFactory = null;
      }
    });

    serverVM.invoke(() -> {
      if (serverLauncher != null) {
        serverLauncher.stop();
        serverLauncher = null;
      }
    });
  }

  @Test
  public void destroyMBeanBeforeFederationCompletes() {
    locatorVM.invoke(() -> doAnswer((Answer<Void>) invocation -> {
      serverVM.invoke(() -> {
        Region region = serverLauncher.getCache().getRegion(REGION_NAME);
        region.destroyRegion();
      });

      Region<String, Object> monitoringRegion = invocation.getArgument(2);
      monitoringRegion.destroy(regionMXBeanName.toString());

      assertThat(monitoringRegion.get(regionMXBeanName.toString())).isNull();

      try {
        invocation.callRealMethod();
      } catch (Exception e) {
        if (!locatorLauncher.getCache().isClosed()) {
          errorCollector.addError(e);
        }
      }

      return null;
    })
        .when(proxyFactory).createProxy(any(), eq(regionMXBeanName), any(), any()));

    serverVM.invoke(() -> {
      serverLauncher.getCache().createRegionFactory(REPLICATE).create(REGION_NAME);
    });

    locatorVM.invoke(() -> {
      await().untilAsserted(
          () -> verify(proxyFactory).createProxy(any(), eq(regionMXBeanName), any(), any()));
    });
  }

  private int startLocator() throws IOException {
    System.setProperty(FEDERATING_MANAGER_FACTORY_PROPERTY,
        FederatingManagerFactoryWithSpy.class.getName());

    locatorLauncher = new LocatorLauncher.Builder()
        .setMemberName(locatorName)
        .setPort(0)
        .setWorkingDirectory(temporaryFolder.newFolder(locatorName).getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(JMX_MANAGER_PORT, "0")
        .build();

    locatorLauncher.start();

    Cache cache = locatorLauncher.getCache();

    SystemManagementService service =
        (SystemManagementService) ManagementService.getManagementService(cache);
    service.startManager();
    FederatingManager federatingManager = service.getFederatingManager();
    proxyFactory = federatingManager.proxyFactory();

    return locatorLauncher.getPort();
  }

  private void startServer() throws IOException {
    serverLauncher = new ServerLauncher.Builder()
        .setDisableDefaultServer(true)
        .setMemberName(serverName)
        .setWorkingDirectory(temporaryFolder.newFolder(serverName).getAbsolutePath())
        .set(HTTP_SERVICE_PORT, "0")
        .set(LOCATORS, "localHost[" + locatorPort + "]")
        .build();

    serverLauncher.start();
  }

  private static class FederatingManagerFactoryWithSpy implements FederatingManagerFactory {

    public FederatingManagerFactoryWithSpy() {
      // must be public for instantiation by reflection
    }

    @Override
    public FederatingManager create(ManagementResourceRepo repo, InternalDistributedSystem system,
        SystemManagementService service, InternalCache cache, StatisticsFactory statisticsFactory,
        StatisticsClock statisticsClock, MBeanProxyFactory proxyFactory, MemberMessenger messenger,
        Supplier<ExecutorService> executorServiceSupplier) {
      return new FederatingManager(repo, system, service, cache, statisticsFactory,
          statisticsClock, spy(proxyFactory), messenger, executorServiceSupplier);
    }
  }
}
