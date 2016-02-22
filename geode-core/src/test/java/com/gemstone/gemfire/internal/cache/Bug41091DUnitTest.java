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
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.InitialImageOperation.RequestImageMessage;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * 
 * This class tests that bucket regions can handle
 * a failure of the GII target during GII.
 * @author dsmith
 *
 */
public class Bug41091DUnitTest extends CacheTestCase {

  /**
   * @param name
   */
  public Bug41091DUnitTest(String name) {
    super(name);
  }
  
  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }
  
  public void test() {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
    //We need to use our own locator because we need enable network partition detection.
    startLocatorInVM(vm3, locatorPort);
    try {
    
    final SerializableRunnable createRegion = new SerializableRunnable("create the region") {
      
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof RequestImageMessage) {
              RequestImageMessage rim = (RequestImageMessage) message;
              Region region = getCache().getRegion(rim.regionPath);
              if(region instanceof BucketRegion) {
//We can no longer do any puts until the bucket is completely created,
//so this will hang
//                getCache().getRegion("region").put(113, "b");
                getCache().close();
              }
            }
          }
        });
   
        Properties props = new Properties();
        props.setProperty(DistributionConfig.ENABLE_NETWORK_PARTITION_DETECTION_NAME, "true");
        props.setProperty(DistributionConfig.LOCATORS_NAME, NetworkUtils.getServerHostName(host) + "[" + locatorPort + "]");
        getSystem(props);
        
        
        Cache cache = getCache();
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        af.setPartitionAttributes(paf.create());
        cache.createRegion("region", af.create());
      }
    };
    vm0.invoke(createRegion);
    vm1.invoke(createRegion);
    
    vm2.invoke(new SerializableRunnable("create an entry") {
      
      public void run() {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.ENABLE_NETWORK_PARTITION_DETECTION_NAME, "true");
        props.setProperty(DistributionConfig.LOCATORS_NAME, NetworkUtils.getServerHostName(host) + "[" + locatorPort + "]");
        getSystem(props);
        Cache cache = getCache();
        AttributesFactory af = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        paf.setLocalMaxMemory(0);
        af.setPartitionAttributes(paf.create());
        Region region = cache.createRegion("region", af.create());
        region.put(Integer.valueOf(0), "a");
      }
    });
    } finally {
      SerializableRunnable stopLocator = 
        new SerializableRunnable("Stop locator") {
            public void run() {
              assertTrue(Locator.hasLocator());
              Locator.getLocator().stop();
              assertFalse(Locator.hasLocator());
            }
          };
      vm3.invoke(stopLocator);
      
    }
  }
  
  protected void startLocatorInVM(final VM vm, final int locatorPort) {
    vm.invoke(new SerializableRunnable("Create Locator") {

      final String testName= getUniqueName();
      public void run() {
        disconnectFromDS();
        Properties props = new Properties();
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOG_LEVEL_NAME, LogWriterUtils.getDUnitLogLevel());
        props.setProperty(DistributionConfig.ENABLE_NETWORK_PARTITION_DETECTION_NAME, "true");
        props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
        try {
          File logFile = new File(testName + "-locator" + locatorPort
              + ".log");
          InetAddress bindAddr = null;
          try {
            bindAddr = InetAddress.getByName(NetworkUtils.getServerHostName(vm.getHost()));
          } catch (UnknownHostException uhe) {
            Assert.fail("While resolving bind address ", uhe);
          }
          Locator locator = Locator.startLocatorAndDS(locatorPort, logFile, bindAddr, props);
        } catch (IOException ex) {
          Assert.fail("While starting locator on port " + locatorPort, ex);
        }
      }
    });
  }
}
