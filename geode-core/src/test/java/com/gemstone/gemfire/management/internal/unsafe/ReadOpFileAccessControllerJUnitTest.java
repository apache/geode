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
package com.gemstone.gemfire.management.internal.unsafe;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import javax.management.*;
import javax.management.remote.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Test ReadOnly operations are accesible from RMI Connector with readOnly user
 *
 */
@Category(IntegrationTest.class)
public class ReadOpFileAccessControllerJUnitTest {
  
  private GemFireCacheImpl cache = null;
  private DistributedSystem ds = null;
  private String hostname;
  private int port = 9999;
  private JMXConnectorServer rmiConnector=null;
  private JMXConnector connector =null;
  private Registry registry = null;

  public static final String SERVICE_URLPREFIX = "service:jmx:rmi:///jndi/rmi:";
  private static final String NEW_LINE = System.getProperty("line.separator");

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();
  
  @Before
  public void setUp() throws Exception {
    Properties pr = getProperties();
    ds = getSystem(pr);
    cache = (GemFireCacheImpl) CacheFactory.create(ds);
    hostname = InetAddress.getLocalHost().getCanonicalHostName();
  }
  
  @After
  public void tearDown() throws Exception {    
    connector.close();
    rmiConnector.stop();
    cache.close();
    ds.disconnect();
    UnicastRemoteObject.unexportObject(registry, true);
  }
  
  private Properties getProperties() {
    Properties pr = new Properties();
    pr.put(MCAST_PORT, "0");
    pr.put(LOCATORS, "");
    //pr.put("jmx-manager", "true");
    //pr.put("jmx-manager-start", "true");
    return pr;
  }

  @Test
  public void testReadOnlyOperations() throws IOException, InstanceNotFoundException, ReflectionException, MBeanException {
    ManagementService service = ManagementService.getExistingManagementService(GemFireCacheImpl.getInstance());
    String accessFileName = createAccessFile();
    String passwordFileName = createPasswordFile();
    createConnector(accessFileName, passwordFileName);
    
    MBeanServerConnection server = connectToRmiConnector();
    DistributedMember member=  cache.getMyId();
    
    assertNotNull(server.invoke(service.getMemberMBeanName(member), "listRegions", null,null));
    assertNotNull(server.invoke(service.getMemberMBeanName(member), "listGemFireProperties", null,null));
    assertNotNull(server.invoke(service.getMemberMBeanName(member), "listConnectedGatewayReceivers", null,null));
    assertNotNull(server.invoke(service.getMemberMBeanName(member), "listConnectedGatewaySenders", null,null));
    
    assertNotNull(server.invoke(service.getMemberMBeanName(member), "showJVMMetrics", null,null));
    assertNotNull(server.invoke(service.getMemberMBeanName(member), "showOSMetrics", null,null));
    
    assertNotNull(server.invoke(service.getMemberMBeanName(member), "fetchJvmThreads", null,null));
    
    assertNotNull(server.invoke(service.getMemberMBeanName(member), "viewLicense", null,null));
    
    //TODO QueryData : Start Manager fails due to  #50280 : Can not start manager inside a loner    
    try {
      server.invoke(service.getMemberMBeanName(member), "compactAllDiskStores",
          null, null);
      fail("Admin operation accessible to readonly user");
    } catch (SecurityException e) {
     //ok 
    }
 
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private MBeanServerConnection connectToRmiConnector() throws MalformedURLException, IOException {
    String serviceUrl = SERVICE_URLPREFIX + "//" + hostname + ":" + port + "/jmxconnector"; 
    String[] creds = {"user", "user"};
    Map env = new HashMap();
    env.put(JMXConnector.CREDENTIALS, creds);
    connector = JMXConnectorFactory.connect(new JMXServiceURL(serviceUrl),env);
    return connector.getMBeanServerConnection();    
  }

  private void createConnector(String accessFileName, String pwFile) throws IOException {
    registry = LocateRegistry.createRegistry(port);
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    String serviceUrl = SERVICE_URLPREFIX + "//" + hostname + ":" + port
        + "/jmxconnector";
    System.out.println("Server service url " + serviceUrl);
    final JMXServiceURL jmxServiceUrl = new JMXServiceURL(serviceUrl);

    final HashMap<String, Object> env = new HashMap<String, Object>();
    env.put("jmx.remote.x.password.file", pwFile);

    ReadOpFileAccessController controller = new ReadOpFileAccessController(
        accessFileName);
    controller.setMBeanServer(mbs);    
    rmiConnector = JMXConnectorServerFactory.newJMXConnectorServer(jmxServiceUrl,
        env, controller);
    rmiConnector.start();
  }
  
  private String createAccessFile() throws IOException {
    File file = tempFolder.newFile("jmxremote.access");
    BufferedWriter writer = new BufferedWriter(new FileWriter(file));
    writer.append("admin readwrite");
    writer.append(NEW_LINE);
    writer.append("user readonly");
    writer.append(NEW_LINE);
    writer.flush();
    writer.close();
    return file.getAbsolutePath();
  }
  
  private String createPasswordFile() throws IOException {
    File file = tempFolder.newFile("jmxremote.password");
    BufferedWriter writer = new BufferedWriter(new FileWriter(file));
    writer.append("admin admin");
    writer.append(NEW_LINE);
    writer.append("user user");
    writer.append(NEW_LINE);
    writer.flush();
    writer.close();
    return file.getAbsolutePath();
  }

  private static DistributedSystem getSystem(Properties properties) {
    return DistributedSystem.connect(properties);
  }

}
