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
package com.gemstone.gemfire.management.internal.configuration;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.management.internal.configuration.domain.Configuration;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity.XmlEntityBuilder;
import com.gemstone.gemfire.management.internal.configuration.handlers.ConfigurationRequestHandler;
import com.gemstone.gemfire.management.internal.configuration.messages.ConfigurationRequest;
import com.gemstone.gemfire.management.internal.configuration.messages.ConfigurationResponse;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/***
 * Tests the starting up of shared configuration, installation of {@link ConfigurationRequestHandler}
 * 
 * @author bansods
 *
 */
public class SharedConfigurationDUnitTest extends CacheTestCase {
  private static final long serialVersionUID = 1L;
  private static final String REGION1 = "region1";
  private static final int TIMEOUT = 10000;
  private static final int INTERVAL = 500;
  private static final String DISKSTORENAME = "diskStore1";

//  private static final VM locator1Vm = Host.getHost(0).getVM(1);
//  private static final VM locator2Vm = Host.getHost(0).getVM(2);
//  private static final VM dataMemberVm = Host.getHost(0).getVM(3);

  public static final SerializableRunnable locatorCleanup = new SerializableRunnable() {
    @Override
    public void run() {
      InternalLocator locator = InternalLocator.getLocator();
      if (locator != null) {
        SharedConfiguration sharedConfig = locator.getSharedConfiguration();
        if (sharedConfig != null) {
          sharedConfig.destroySharedConfiguration();
        }
        locator.stop();
      }
      disconnectAllFromDS();
    }
  };
  
  
  public SharedConfigurationDUnitTest(String name) {
    super(name);
  }
  
  public void testGetHostedLocatorsWithSharedConfiguration() {
    disconnectAllFromDS();
    final VM locator1Vm = Host.getHost(0).getVM(1);
    final VM locator2Vm = Host.getHost(0).getVM(2);
  
    final String testName = "testGetHostedLocatorsWithSharedConfiguration";
//    final VM locator3Vm = Host.getHost(0).getVM(3);
    
    final int []ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    
    //final int locator1Port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final int locator1Port = ports[0];
    final String locator1Name = "locator1" + locator1Port;
    locator1Vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        final File locatorLogFile = new File(testName + "-locator-" + locator1Port + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(DistributionConfig.NAME_NAME, locator1Name);
        locatorProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        locatorProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
        locatorProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator1Port, locatorLogFile, null,
              locatorProps);

          WaitCriterion wc = new WaitCriterion() {
            @Override
            public boolean done() {
              return locator.isSharedConfigurationRunning();
            }

            @Override
            public String description() {
              return "Waiting for shared configuration to be started";
            }
          };
          Wait.waitForCriterion(wc, TIMEOUT, INTERVAL, true);
        } catch (IOException ioex) {
          fail("Unable to create a locator with a shared configuration");
        }
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        InternalDistributedMember me = cache.getMyId();
        DM dm = cache.getDistributionManager();
        Map<InternalDistributedMember, Collection<String>> hostedLocators = dm.getAllHostedLocators();
        assertFalse(hostedLocators.isEmpty());
        Map<InternalDistributedMember, Collection<String>> hostedLocatorsWithSharedConfiguration = dm.getAllHostedLocatorsWithSharedConfiguration();
        assertFalse(hostedLocatorsWithSharedConfiguration.isEmpty());
        
        assertNotNull(hostedLocators.get(me));
        assertNotNull(hostedLocatorsWithSharedConfiguration.get(me));
        return null;
      }
    });
    
    //final int locator2Port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final int locator2Port = ports[1];
    final String locator2Name = "locator2" + locator2Port;

    locator2Vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws IOException {
        final File locatorLogFile = new File(testName + "-locator-" + locator2Port + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(DistributionConfig.NAME_NAME, locator2Name);
        locatorProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        locatorProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
        locatorProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locator1Port);
        locatorProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
        final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator2Port, locatorLogFile, null,
            locatorProps);

        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        InternalDistributedMember me = cache.getMyId();
        DM dm = cache.getDistributionManager();
        Map<InternalDistributedMember, Collection<String>> hostedLocators = dm.getAllHostedLocators();
        assertFalse(hostedLocators.isEmpty());
        Map<InternalDistributedMember, Collection<String>> hostedLocatorsWithSharedConfiguration = dm.getAllHostedLocatorsWithSharedConfiguration();
        assertFalse(hostedLocatorsWithSharedConfiguration.isEmpty());
        assertNotNull(hostedLocators.get(me));
        assertNull(hostedLocatorsWithSharedConfiguration.get(me));
        assertTrue(hostedLocators.size() == 2);
        assertTrue(hostedLocatorsWithSharedConfiguration.size() == 1);
        
        Set<InternalDistributedMember> locatorsWithSharedConfig = hostedLocatorsWithSharedConfiguration.keySet();
        Set<String> locatorsWithSharedConfigNames = new HashSet<String>();
        
        for (InternalDistributedMember locatorWithSharedConfig : locatorsWithSharedConfig) {
          locatorsWithSharedConfigNames.add(locatorWithSharedConfig.getName());
        }
        assertTrue(locatorsWithSharedConfigNames.contains(locator1Name));
        
        return null;
      }
    });
    
    locator1Vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        InternalLocator locator = (InternalLocator) Locator.getLocator();
        SharedConfiguration sharedConfig = locator.getSharedConfiguration();
        sharedConfig.destroySharedConfiguration();
        locator.stop();
        return null;
      }
    });
    
    locator2Vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        InternalDistributedMember me = cache.getMyId();
        DM dm = cache.getDistributionManager();
        Map<InternalDistributedMember, Collection<String>> hostedLocators = dm.getAllHostedLocators();
        assertFalse(hostedLocators.isEmpty());
        Map<InternalDistributedMember, Collection<String>> hostedLocatorsWithSharedConfiguration = dm.getAllHostedLocatorsWithSharedConfiguration();
        assertTrue(hostedLocatorsWithSharedConfiguration.isEmpty());
        assertNotNull(hostedLocators.get(me));
        assertNull(hostedLocatorsWithSharedConfiguration.get(me));
        assertTrue(hostedLocators.size() == 1);
        assertTrue(hostedLocatorsWithSharedConfiguration.size() == 0);
        return null;
      }
    });
    
//    locator2Vm.invoke(new SerializableCallable() {
//      public Object call() {
//        InternalLocator locator = (InternalLocator) Locator.getLocator();
//        SharedConfiguration sharedConfig = locator.getSharedConfiguration();
//        if (sharedConfig != null) {
//          sharedConfig.destroySharedConfiguration();
//        }
//        locator.stop();
//        return null;
//      }
//    });
  }
  
  public void testSharedConfigurationService() {
    disconnectAllFromDS();
    // Start the Locator and wait for shared configuration to be available
    final String testGroup = "G1";
    final String clusterLogLevel = "error";
    final String groupLogLevel = "fine";
    final String testName = "testSharedConfigurationService";
    
    final VM locator1Vm = Host.getHost(0).getVM(1);
    final VM locator2Vm = Host.getHost(0).getVM(3);
    final VM dataMemberVm = Host.getHost(0).getVM(2);
    final int [] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    final int locator1Port = ports[0];
    
    locator1Vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        final File locatorLogFile = new File(testName + "-locator-" + locator1Port + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(DistributionConfig.NAME_NAME, "Locator1");
        locatorProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        locatorProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
        locatorProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator1Port, locatorLogFile, null,
              locatorProps);
          WaitCriterion wc = new WaitCriterion() {
            @Override
            public boolean done() {
              return locator.isSharedConfigurationRunning();
            }

            @Override
            public String description() {
              return "Waiting for shared configuration to be started";
            }
          };
          Wait.waitForCriterion(wc, TIMEOUT, INTERVAL, true);
        } catch (IOException ioex) {
          fail("Unable to create a locator with a shared configuration");
        }

        return null;
      }
    });
    
    dataMemberVm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        localProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locator1Port);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, testGroup);
        getSystem(localProps);
        Cache cache = getCache();
        assertNotNull(cache);
        DiskStoreFactory dsFactory = cache.createDiskStoreFactory();
        File dsDir = new File("dsDir");
        if (!dsDir.exists()) {
          dsDir.mkdir();
        }
        dsFactory.setDiskDirs(new File[] {dsDir});
        dsFactory.create(DISKSTORENAME);
        
        RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.REPLICATE);
        regionFactory.create(REGION1);
        
        XmlEntity xmlEntity = new XmlEntity(CacheXml.REGION, "name", REGION1);
        final SharedConfigurationWriter scw = new SharedConfigurationWriter();
        assertTrue(scw.addXmlEntity(xmlEntity, new String[] {testGroup}));
        
        xmlEntity = new XmlEntity(CacheXml.DISK_STORE, "name", DISKSTORENAME);
        assertTrue(scw.addXmlEntity(xmlEntity, new String[] {testGroup}));
        //Modify property and cache attributes
        Properties clusterProperties = new Properties();
        clusterProperties.setProperty(DistributionConfig.LOG_LEVEL_NAME, clusterLogLevel);
        XmlEntity cacheEntity = XmlEntity.builder().withType(CacheXml.CACHE).build();
        Map<String, String> cacheAttributes = new HashMap<String, String>();
        cacheAttributes.put(CacheXml.COPY_ON_READ, "true");
        
        //assertTrue(scw.modifyProperties(clusterProperties, null));
        assertTrue(scw.modifyPropertiesAndCacheAttributes(clusterProperties, cacheEntity, null));

        clusterProperties.setProperty(DistributionConfig.LOG_LEVEL_NAME, groupLogLevel);
        assertTrue(scw.modifyPropertiesAndCacheAttributes(clusterProperties, cacheEntity, new String[]{testGroup}));

        //Add a jar
        byte[][] jarBytes = new byte[1][];
        jarBytes[0] = "Hello".getBytes();
        assertTrue(scw.addJars(new String[]{"foo.jar"}, jarBytes, null));
        
        //Add a jar for the group
        jarBytes = new byte[1][];
        jarBytes[0] = "Hello".getBytes();
        assertTrue(scw.addJars(new String[]{"bar.jar"}, jarBytes, new String[]{testGroup}));
        return null;
      }
    });
   
    final int locator2Port = ports[1];
    
    //Create another locator in VM2
    locator2Vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {

        final File locatorLogFile = new File(testName + "-locator-" + locator2Port + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(DistributionConfig.NAME_NAME, "Locator2");
        locatorProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        locatorProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
        locatorProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        locatorProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locator1Port);
        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator2Port, locatorLogFile, null,
              locatorProps);

          WaitCriterion wc = new WaitCriterion() {
            @Override
            public boolean done() {
              return locator.isSharedConfigurationRunning();
            }

            @Override
            public String description() {
              return "Waiting for shared configuration to be started";
            }
          };
          Wait.waitForCriterion(wc, TIMEOUT, INTERVAL, true);
        } catch (IOException ioex) {
          fail("Unable to create a locator with a shared configuration");
        }
        
        InternalLocator locator = (InternalLocator) Locator.getLocator();
        SharedConfiguration sharedConfig = locator.getSharedConfiguration();
        Map<String, Configuration> entireConfiguration = sharedConfig.getEntireConfiguration();
        Configuration clusterConfig = entireConfiguration.get(SharedConfiguration.CLUSTER_CONFIG);
        assertNotNull(clusterConfig);
        assertNotNull(clusterConfig.getJarNames());
        assertTrue(clusterConfig.getJarNames().contains("foo.jar"));
        assertTrue(clusterConfig.getGemfireProperties().getProperty(DistributionConfig.LOG_LEVEL_NAME).equals(clusterLogLevel));
        assertNotNull(clusterConfig.getCacheXmlContent());
        
        Configuration testGroupConfiguration = entireConfiguration.get(testGroup);
        assertNotNull(testGroupConfiguration);
        assertNotNull(testGroupConfiguration.getJarNames());
        assertTrue(testGroupConfiguration.getJarNames().contains("bar.jar"));
        assertTrue(testGroupConfiguration.getGemfireProperties().getProperty(DistributionConfig.LOG_LEVEL_NAME).equals(groupLogLevel));
        assertNotNull(testGroupConfiguration.getCacheXmlContent());
        assertTrue(testGroupConfiguration.getCacheXmlContent().contains(REGION1));
        
        Object[] jarData = sharedConfig.getAllJars(entireConfiguration.keySet());
        String[] jarNames = (String[]) jarData[0];
        byte[][] jarBytes = (byte[][]) jarData[1];

        assertNotNull(jarNames);
        assertNotNull(jarBytes);
        
        return null;
      }
    });
    
    dataMemberVm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws UnknownHostException, IOException, ClassNotFoundException {
        SharedConfigurationWriter scw = new SharedConfigurationWriter();
        scw.deleteXmlEntity(new XmlEntity(CacheXml.REGION, "name", REGION1), new String[]{testGroup});
        scw.deleteJars(new String []{"foo.jar"}, null);
        scw.deleteJars(null, null);
        
        Set<String> groups = new HashSet<String>();
        groups.add(testGroup);
        ConfigurationRequest configRequest = new ConfigurationRequest(groups);
        ConfigurationResponse configResponse = (ConfigurationResponse) TcpClient.requestToServer(InetAddress.getByName("localhost"), locator2Port, configRequest, 1000);
        assertNotNull(configResponse);
        
        Map<String, Configuration> requestedConfiguration = configResponse.getRequestedConfiguration();
        Configuration clusterConfiguration = requestedConfiguration.get(SharedConfiguration.CLUSTER_CONFIG);
        assertNotNull(clusterConfiguration);
        assertNull(configResponse.getJarNames());
        assertNull(configResponse.getJars());
        assertTrue(clusterConfiguration.getJarNames().isEmpty());
        assertTrue(clusterConfiguration.getGemfireProperties().getProperty(DistributionConfig.LOG_LEVEL_NAME).equals(clusterLogLevel));
        
        Configuration testGroupConfiguration = requestedConfiguration.get(testGroup);
        assertNotNull(testGroupConfiguration);
        assertFalse(testGroupConfiguration.getCacheXmlContent().contains(REGION1));
        assertTrue(testGroupConfiguration.getJarNames().isEmpty());
        assertTrue(testGroupConfiguration.getGemfireProperties().getProperty(DistributionConfig.LOG_LEVEL_NAME).equals(groupLogLevel));
        
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        Map<InternalDistributedMember, Collection<String>> locatorsWithSharedConfiguration = cache.getDistributionManager().getAllHostedLocatorsWithSharedConfiguration();
        assertFalse(locatorsWithSharedConfiguration.isEmpty());
        assertTrue(locatorsWithSharedConfiguration.size() == 2);
        Set<InternalDistributedMember> locatorMembers = locatorsWithSharedConfiguration.keySet();
        for (InternalDistributedMember locatorMember : locatorMembers) {
          System.out.println(locatorMember);
        }
        return null;
      }
    });
  }    
  
  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    for (int i=0; i<4; i++) {
      Host.getHost(0).getVM(i).invoke(SharedConfigurationDUnitTest.locatorCleanup);
    }
  }
}