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

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_TTL;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;


public class DistributedMulticastRegionDUnitTest extends JUnit4CacheTestCase {

  int locatorVM = 3;
  String mcastport = "0";
  String mcastttl = "0";

  private int locatorPort;

  @Before
  public void setup() {
    mcastport = String.valueOf(AvailablePortHelper.getRandomAvailableUDPPort());
  }

  @Override
  public final void preSetUp() throws Exception {
    clean();
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    clean();
  }

  private void clean() {
    SerializableRunnable cleanVM = new CacheSerializableRunnable("clean VM") {
      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
      }
    };
    Invoke.invokeInEveryVM(cleanVM);
  }

  @Test
  public void testMulticastEnabled() {
    final String name = "mcastRegion";
    SerializableRunnable create = new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        createRegion(name, getRegionAttributes());
      }
    };

    locatorPort = startLocator();
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);

    vm0.invoke(create);
    vm1.invoke(create);

    SerializableRunnable doPuts = new CacheSerializableRunnable("do put") {
      @Override
      public void run2() throws CacheException {
        final Region region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 50; i++) {
          region.put(i, i);
        }
      }
    };

    vm0.invoke(doPuts);
    vm0.invoke(() -> validateMulticastOpsAfterRegionOps());
    vm1.invoke(() -> validateMulticastOpsAfterRegionOps());

    closeLocator();
  }

  @Test
  public void testMulticastAfterReconnect() {
    final String name = "mcastRegion";
    SerializableRunnable create = new CacheSerializableRunnable("Create Region") {
      @Override
      public void run2() throws CacheException {
        createRegion(name, getRegionAttributes());
      }
    };

    locatorPort = startLocator();
    Host host = Host.getHost(0);

    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    vm0.invoke(create);
    vm1.invoke(create);

    SerializableRunnable doPuts = new CacheSerializableRunnable("do put") {
      @Override
      public void run2() throws CacheException {
        final Region region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 50; i++) {
          region.put(i, i);
        }
      }
    };

    vm0.invoke(doPuts);

    DistributedTestUtils.crashDistributedSystem(vm1);
    vm0.invoke(doPuts);
    vm1.invoke("wait for vm1 to reconnect", () -> {
      basicGetCache().waitUntilReconnected(30, TimeUnit.SECONDS);
      assertNotNull(basicGetCache().getReconnectedCache());
      cache = (InternalCache) basicGetCache().getReconnectedCache();
      system = cache.getInternalDistributedSystem();
    });

    // after vm1 reconnects it should have the correct multicast digest and operations
    // on the cache region should be acknowledged.
    AsyncInvocation<Object> asyncInvocation = vm0.invokeAsync(doPuts);
    GeodeAwaitility.await().until(asyncInvocation::isDone);
    Assertions.assertThat(asyncInvocation.getException()).isNull();

    // after vm1 reconnects it should respond to multicast destroy-region messages
    asyncInvocation = vm0.invokeAsync(() -> {
      GeodeAwaitility.await().until(() -> {
        getCache().close();
        return true;
      });
    });
    GeodeAwaitility.await().until(asyncInvocation::isDone);
    Assertions.assertThat(asyncInvocation.getException()).isNull();

    vm1.invoke(() -> validateMulticastOpsAfterRegionOps());

    closeLocator();
  }

  private static class TestObjectThrowsException implements PdxSerializable {

    String name = "TestObjectThrowsException";

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("name", name);
    }

    @Override
    public void fromData(PdxReader reader) {
      throw new RuntimeException("Unable to deserialize message ");
    }
  }

  @Test
  public void testMulticastWithRegionOpsException() {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);

    try {
      final String name = "mcastRegion";

      locatorPort = startLocator();

      vm0.invoke("setSysProps", () -> setSysProps());
      vm1.invoke("setSysProps", () -> setSysProps());

      // 1. start locator with mcast port
      vm0.invoke("createRegion", () -> {
        createRegion(name, getRegionAttributes());
        return "";
      });
      vm1.invoke("createRegion", () -> {
        createRegion(name, getRegionAttributes());
        return "";
      });

      vm0.invoke("do Put() with exception test", () -> {
        final Region region = getRootRegion().getSubregion(name);
        boolean gotReplyException = false;
        for (int i = 0; i < 1; i++) {
          try {
            region.put(i, new TestObjectThrowsException());
          } catch (PdxSerializationException e) {
            gotReplyException = true;
          } catch (Exception e) {
            region.getCache().getLogger().info("Got exception of type " + e.getClass().toString());
          }
        }
        assertTrue("We should have got ReplyException ", gotReplyException);
      });

      vm0.invoke("validateMulticastOpsAfterRegionOps", () -> validateMulticastOpsAfterRegionOps());
      vm1.invoke("validateMulticastOpsAfterRegionOps", () -> validateMulticastOpsAfterRegionOps());

      closeLocator();
    } finally {
      SerializableRunnable unsetSysProp = new CacheSerializableRunnable("Create Region") {
        @Override
        public void run2() throws CacheException {
          CachedDeserializableFactory.STORE_ALL_VALUE_FORMS = false;
        }
      };
      vm0.invoke(unsetSysProp);
      vm1.invoke(unsetSysProp);
    }
  }

  private void setSysProps() {
    CachedDeserializableFactory.STORE_ALL_VALUE_FORMS = true;
  }

  protected <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PRELOADED);
    factory.setEarlyAck(false);
    factory.setConcurrencyChecksEnabled(false);
    factory.setMulticastEnabled(true);
    return factory.create();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties p = new Properties();
    p.put(NAME, "vm" + VM.getCurrentVMNum());
    p.put(DISABLE_AUTO_RECONNECT, "false");
    p.put(MAX_WAIT_TIME_RECONNECT, "20");
    p.put(STATISTIC_SAMPLING_ENABLED, "true");
    p.put(STATISTIC_ARCHIVE_FILE, "multicast" + OSProcess.getId());
    p.put(ENABLE_TIME_STATISTICS, "true");
    p.put(MCAST_PORT, mcastport);
    p.put(MCAST_TTL, mcastttl);
    p.put(LOCATORS, "localhost[" + locatorPort + "]");
    p.put(LOG_LEVEL, "info");
    addDSProps(p);
    return p;
  }

  protected void addDSProps(Properties p) {}

  protected void validateMulticastOpsAfterRegionOps() {
    long writes = getGemfireCache().getDistributionManager().getStats().getMcastWrites();
    long reads = getGemfireCache().getDistributionManager().getStats().getMcastReads();
    assertTrue("Should have multicast writes or reads. Writes=  " + writes + " ,read= " + reads,
        writes > 0 || reads > 0);

    validateUDPEncryptionStats();
  }

  protected void validateUDPEncryptionStats() {
    long encrptTime =
        getGemfireCache().getDistributionManager().getStats().getUDPMsgEncryptionTime();
    long decryptTime =
        getGemfireCache().getDistributionManager().getStats().getUDPMsgDecryptionTime();
    assertTrue("Should have multicast writes or reads. encrptTime=  " + encrptTime
        + " ,decryptTime= " + decryptTime, encrptTime == 0 && decryptTime == 0);
  }

  private void validateMulticastOpsBeforeRegionOps() {
    long writes = getGemfireCache().getDistributionManager().getStats().getMcastWrites();
    long reads = getGemfireCache().getDistributionManager().getStats().getMcastReads();
    long total = writes + reads;
    assertTrue("Should not have any multicast writes or reads before region ops. Writes=  " + writes
        + " ,read= " + reads, total == 0);
  }

  private int startLocator() {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    final int locatorPort = ports[0];

    VM locator1Vm = Host.getHost(0).getVM(locatorVM);;
    locator1Vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        final File locatorLogFile =
            new File(getTestMethodName() + "-locator-" + locatorPort + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(NAME, "LocatorWithMcast");
        locatorProps.setProperty(MCAST_PORT, mcastport);
        locatorProps.setProperty(MCAST_TTL, mcastttl);
        locatorProps.setProperty(LOG_LEVEL, "info");
        addDSProps(locatorProps);
        // locatorProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        try {
          final InternalLocator locator =
              (InternalLocator) Locator.startLocatorAndDS(locatorPort, null, null, locatorProps);
          System.out.println("test Locator started " + locatorPort);
        } catch (IOException ioex) {
          fail("Unable to create a locator with a shared configuration");
        }
        return null;
      }
    });
    return locatorPort;
  }

  private void closeLocator() {
    VM locator1Vm = Host.getHost(0).getVM(locatorVM);;
    SerializableRunnable locatorCleanup = new SerializableRunnable() {
      @Override
      public void run() {
        System.out.println("test Locator closing " + locatorPort);;
        InternalLocator locator = InternalLocator.getLocator();
        if (locator != null) {
          locator.stop();
          System.out.println("test Locator closed " + locatorPort);;
        }
      }
    };
    locator1Vm.invoke(locatorCleanup);
  }

}
