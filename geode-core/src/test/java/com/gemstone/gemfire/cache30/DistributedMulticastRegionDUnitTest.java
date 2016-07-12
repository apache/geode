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
package com.gemstone.gemfire.cache30;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxSerializationException;
import com.gemstone.gemfire.pdx.PdxWriter;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class DistributedMulticastRegionDUnitTest extends JUnit4CacheTestCase {

  static int locatorVM = 3;
  static String mcastport = "42786";
  static String mcastttl = "0";

  private int locatorPort;

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
      public void run2() throws CacheException {
        createRegion(name, getRegionAttributes());
      }
    };

    locatorPort = startLocator();
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    //1. start locator with mcast port
    vm0.invoke(create);
    vm1.invoke(create);
    //There is possibility that you may get this packet from other tests
    /*
    SerializableRunnable validateMulticastBeforeRegionOps =
        new CacheSerializableRunnable("validateMulticast before region ops") {
            public void run2() throws CacheException {
              validateMulticastOpsBeforeRegionOps();
            }
        };
      
    vm0.invoke(validateMulticastBeforeRegionOps);
    vm1.invoke(validateMulticastBeforeRegionOps);
    */

    SerializableRunnable doPuts = new CacheSerializableRunnable("do put") {
      public void run2() throws CacheException {
        final Region region = getRootRegion().getSubregion(name);
        for (int i = 0; i < 5; i++) {
          region.put(i, i);
        }
      }
    };

    vm0.invoke(doPuts);

    SerializableRunnable validateMulticastAfterRegionOps = new CacheSerializableRunnable("validateMulticast after region ops") {
      public void run2() throws CacheException {
        validateMulticastOpsAfterRegionOps();
      }
    };

    vm0.invoke(validateMulticastAfterRegionOps);
    vm1.invoke(validateMulticastAfterRegionOps);

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
      //      vm1.invoke("setSysProps", () -> setSysProps());
      setSysProps();
      //1. start locator with mcast port
      vm0.invoke("createRegion", () -> {
        createRegion(name, getRegionAttributes());
        return "";
      });
      //      vm1.invoke("createRegion", () -> createRegion(name,getRegionAttributes()));
      createRegion(name, getRegionAttributes());

      //There is possibility that you may get this packet from other tests
      /*
      SerializableRunnable validateMulticastBeforeRegionOps =
          new CacheSerializableRunnable("validateMulticast before region ops") {
              public void run2() throws CacheException {
                validateMulticastOpsBeforeRegionOps();
              }
          };
        
      vm0.invoke(validateMulticastBeforeRegionOps);
      vm1.invoke(validateMulticastBeforeRegionOps);
      */

      SerializableRunnable doPuts = new CacheSerializableRunnable("do put") {
        public void run2() throws CacheException {
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
          assertTrue("We should have got ReplyEception ", gotReplyException);
        }
      };

      vm0.invoke(doPuts);

      SerializableRunnable validateMulticastAfterRegionOps = new CacheSerializableRunnable("validateMulticast after region ops") {
        public void run2() throws CacheException {
          validateMulticastOpsAfterRegionOps();
        }
      };

      vm0.invoke(validateMulticastAfterRegionOps);
      vm1.invoke(validateMulticastAfterRegionOps);

      closeLocator();
    } finally {
      SerializableRunnable unsetSysProp = new CacheSerializableRunnable("Create Region") {
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

  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PRELOADED);
    factory.setEarlyAck(false);
    factory.setConcurrencyChecksEnabled(false);
    factory.setMulticastEnabled(true);
    return factory.create();
  }

  public Properties getDistributedSystemProperties() {
    Properties p = new Properties();
    p.put(STATISTIC_SAMPLING_ENABLED, "true");
    p.put(STATISTIC_ARCHIVE_FILE, "multicast");
    p.put(MCAST_PORT, mcastport);
    p.put(MCAST_TTL, mcastttl);
    p.put(LOCATORS, "localhost[" + locatorPort + "]");
    p.put(LOG_LEVEL, "info");
    return p;
  }

  private void validateMulticastOpsAfterRegionOps() {
    int writes = getGemfireCache().getDistributionManager().getStats().getMcastWrites();
    int reads = getGemfireCache().getDistributionManager().getStats().getMcastReads();
    assertTrue("Should have multicast writes or reads. Writes=  " + writes + " ,read= " + reads, writes > 0 || reads > 0);
  }

  private void validateMulticastOpsBeforeRegionOps() {
    int writes = getGemfireCache().getDistributionManager().getStats().getMcastWrites();
    int reads = getGemfireCache().getDistributionManager().getStats().getMcastReads();
    int total = writes + reads;
    assertTrue("Should not have any multicast writes or reads before region ops. Writes=  " + writes + " ,read= " + reads, total == 0);
  }

  private int startLocator() {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    final int locatorPort = ports[0];

    VM locator1Vm = Host.getHost(0).getVM(locatorVM);
    locator1Vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        final File locatorLogFile = new File(getTestMethodName() + "-locator-" + locatorPort + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(NAME, "LocatorWithMcast");
        locatorProps.setProperty(MCAST_PORT, mcastport);
        locatorProps.setProperty(MCAST_TTL, mcastttl);
        locatorProps.setProperty(LOG_LEVEL, "info");
        //locatorProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locatorPort, null, null, locatorProps);
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
    VM locator1Vm = Host.getHost(0).getVM(locatorVM);
    ;
    SerializableRunnable locatorCleanup = new SerializableRunnable() {
      @Override
      public void run() {
        System.out.println("test Locator closing " + locatorPort);
        ;
        InternalLocator locator = InternalLocator.getLocator();
        if (locator != null) {
          locator.stop();
          System.out.println("test Locator closed " + locatorPort);
          ;
        }
      }
    };
    locator1Vm.invoke(locatorCleanup);
  }

}
