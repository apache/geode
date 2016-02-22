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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.OffHeapTestUtil;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class DistributedMulticastRegionDUnitTest extends CacheTestCase {

  static int locatorVM = 3;
  static String mcastport = "42786";
  static String mcastttl = "0";
  
  private int locatorPort;

  public DistributedMulticastRegionDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    clean();
    super.setUp();    
  }
  
  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    clean();
  }
  
  private void clean(){
    SerializableRunnable cleanVM =
        new CacheSerializableRunnable("clean VM") {
            public void run2() throws CacheException {
              disconnectFromDS();
            }
        };
    Invoke.invokeInEveryVM(cleanVM);    
  }
  
  public void testMulticastEnabled() {
    final String name = "mcastRegion";
    SerializableRunnable create =
      new CacheSerializableRunnable("Create Region") {
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
    
    SerializableRunnable validateMulticastBeforeRegionOps =
        new CacheSerializableRunnable("validateMulticast before region ops") {
            public void run2() throws CacheException {
              validateMulticastOpsBeforeRegionOps();
            }
        };
      
    vm0.invoke(validateMulticastBeforeRegionOps);
    vm1.invoke(validateMulticastBeforeRegionOps);
    
    SerializableRunnable doPuts =
      new CacheSerializableRunnable("do put") {
          public void run2() throws CacheException {
            final Region region =
                getRootRegion().getSubregion(name);
            for(int i =0 ; i < 5; i++) {
              region.put(i, i);
            }
          }
      };
      
    vm0.invoke(doPuts);
    
    SerializableRunnable validateMulticastAfterRegionOps =
      new CacheSerializableRunnable("validateMulticast after region ops") {
          public void run2() throws CacheException {
            validateMulticastOpsAfterRegionOps();
          }
      };
    
      vm0.invoke(validateMulticastAfterRegionOps);
      vm1.invoke(validateMulticastAfterRegionOps);
   
      closeLocator();      
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
    p.put(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    p.put(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME, "multicast");
    p.put(DistributionConfig.MCAST_PORT_NAME, mcastport);
    p.put(DistributionConfig.MCAST_TTL_NAME, mcastttl);
    p.put(DistributionConfig.LOCATORS_NAME, "localhost[" + locatorPort +"]");
    p.put(DistributionConfig.LOG_LEVEL_NAME, "info");
    return p;
  } 
  
  private void validateMulticastOpsAfterRegionOps() {
    int writes = getGemfireCache().getDistributionManager().getStats().getMcastWrites();
    int reads = getGemfireCache().getDistributionManager().getStats().getMcastReads();
    assertTrue("Should have multicast writes or reads. Writes=  " + writes +  " ,read= " + reads, 
        writes > 0 || reads > 0);
  }
  
  private void validateMulticastOpsBeforeRegionOps() {
    int writes = getGemfireCache().getDistributionManager().getStats().getMcastWrites();
    int reads = getGemfireCache().getDistributionManager().getStats().getMcastReads();
    int total = writes + reads;
    assertTrue("Should not have any multicast writes or reads before region ops. Writes=  " + writes +  " ,read= " + reads, 
        total == 0);
  }
  
  private int startLocator() {
  final int [] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
  final int locatorPort = ports[0];
  
  VM locator1Vm = Host.getHost(0).getVM(locatorVM);;
    locator1Vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        final File locatorLogFile = new File(getTestMethodName() + "-locator-" + locatorPort + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(DistributionConfig.NAME_NAME, "LocatorWithMcast");
        locatorProps.setProperty(DistributionConfig.MCAST_PORT_NAME, mcastport);
        locatorProps.setProperty(DistributionConfig.MCAST_TTL_NAME, mcastttl);
        locatorProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
        //locatorProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locatorPort, null, null,
              locatorProps);
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
        if (locator != null ) {
          locator.stop();
          System.out.println("test Locator closed " + locatorPort);;
        }
      }
    };
    locator1Vm.invoke(locatorCleanup);
  }
  
}
