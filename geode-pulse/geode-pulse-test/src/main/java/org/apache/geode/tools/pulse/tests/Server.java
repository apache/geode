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
package org.apache.geode.tools.pulse.tests;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.registry.LocateRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.internal.security.SecurityServiceFactory;
import org.apache.geode.internal.security.shiro.JMXShiroAuthenticator;
import org.apache.geode.management.internal.security.AccessControlMBean;
import org.apache.geode.management.internal.security.MBeanServerWrapper;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.tools.pulse.internal.data.PulseConstants;

public class Server {

  private static final String DEFAULT_HOST = "127.0.0.1"; // "localhost"
  private static final int DEFAULT_PORT = 9999;
  private final JMXServiceURL url;
  private MBeanServer mbs;
  private JMXConnectorServer cs;
  private String propFile;
  private int jmxPort;
  private String jsonAuthFile;

  public Server(int jmxPort, String properties, String jsonAuthFile) throws Exception {
    this.propFile = properties;
    mbs = ManagementFactory.getPlatformMBeanServer();
    url = new JMXServiceURL(formJMXServiceURLString(DEFAULT_HOST, jmxPort));
    this.jmxPort = jmxPort;
    this.jsonAuthFile = jsonAuthFile;
  }

  private String formJMXServiceURLString(String host, int jmxPort) throws UnknownHostException {
    String jmxSerURL = "";

    InetAddress inetAddr = InetAddress.getByName(host);
    if (inetAddr instanceof Inet4Address) {
      // Create jmx service url for IPv4 address
      jmxSerURL = "service:jmx:rmi://" + host + "/jndi/rmi://" + host + ":" + jmxPort + "/jmxrmi";
    } else if (inetAddr instanceof Inet6Address) {
      // Create jmx service url for IPv6 address
      jmxSerURL =
          "service:jmx:rmi://[" + host + "]/jndi/rmi://[" + host + "]:" + jmxPort + "/jmxrmi";
    }

    return jmxSerURL;
  }

  public void stop() throws Exception {
    cs.stop();
    unloadMBeans();

    if (jsonAuthFile != null)
      mbs.unregisterMBean(new ObjectName(ResourceConstants.OBJECT_NAME_ACCESSCONTROL));
  }

  public void start() throws Exception {

    // Load the beans first, otherwise we get access denied
    loadMBeans();

    if (jsonAuthFile != null) {
      System.setProperty("spring.profiles.active", "pulse.authentication.gemfire");

      Map<String, Object> env = new HashMap<>();

      // set up Shiro Security Manager
      Properties securityProperties = new Properties();
      securityProperties.setProperty(TestSecurityManager.SECURITY_JSON, jsonAuthFile);
      securityProperties.setProperty(SECURITY_MANAGER, TestSecurityManager.class.getName());

      SecurityService securityService = SecurityServiceFactory.create(securityProperties);

      // register the AccessControl bean
      mbs.registerMBean(new AccessControlMBean(securityService),
          new ObjectName(ResourceConstants.OBJECT_NAME_ACCESSCONTROL));

      // wire in the authenticator and authorization
      env.put(JMXConnectorServer.AUTHENTICATOR, new JMXShiroAuthenticator(securityService));
      cs = JMXConnectorServerFactory.newJMXConnectorServer(url, env, mbs);
      cs.setMBeanServerForwarder(new MBeanServerWrapper(securityService));

    } else {
      System.setProperty("spring.profiles.active", "pulse.authentication.default");
      cs = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
    }

    try {
      LocateRegistry.createRegistry(jmxPort);
      System.out.println("RMI registry ready.");
    } catch (Exception e) {
      System.out.println("Exception starting RMI registry:");
      throw e;
    }

    cs.start();
    await()
        .until(() -> cs.isActive());
  }

  private synchronized void loadMBeans() throws Exception {
    JMXProperties props = JMXProperties.getInstance();

    props.load(propFile);

    // Add servers
    String[] servers = getArrayProperty(props, "servers");
    for (String server : servers) {
      addServerMBean(server);
    }

    // Add members
    String[] members = getArrayProperty(props, "members");
    for (String m : members) {
      addMemberMBean(m);
    }

    // Add regions
    String[] regions = getArrayProperty(props, "regions");
    for (String reg : regions) {
      addRegionMBean(reg);
    }
  }

  private synchronized void unloadMBeans() throws Exception {
    JMXProperties props = JMXProperties.getInstance();

    props.load(propFile);

    // remove servers
    String[] servers = getArrayProperty(props, "servers");
    for (String server : servers) {
      removeServerMBean();
    }

    // remove members
    String[] members = getArrayProperty(props, "members");
    for (String m : members) {
      removeMemberMBean(m);
    }

    // remove regions
    String[] regions = getArrayProperty(props, "regions");
    for (String reg : regions) {
      removeRegionMBean(reg);
    }
  }

  private void addMemberMBean(String m) throws InstanceAlreadyExistsException,
      MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException {

    mbs.registerMBean(new Member(m), new ObjectName(Member.OBJECT_NAME + ",member=" + m));
  }


  private void removeMemberMBean(String m)
      throws InstanceNotFoundException,
      MBeanRegistrationException, MalformedObjectNameException {
    mbs.unregisterMBean(new ObjectName(Member.OBJECT_NAME + ",member=" + m));
  }

  private void addRegionMBean(String reg)
      throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException,
      MalformedObjectNameException, NullPointerException {
    Region regionObject = new Region(reg);

    mbs.registerMBean(regionObject, new ObjectName(Region.OBJECT_NAME + ",name=/" + reg));

    for (String member : regionObject.getMembers()) {
      RegionOnMember regionOnMemberObject = new RegionOnMember(regionObject.getFullPath(), member);
      mbs.registerMBean(regionOnMemberObject,
          new ObjectName(
              PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_REGION + regionObject.getFullPath()
                  + PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_MEMBER + member));
    }
  }

  private void removeRegionMBean(String reg)
      throws InstanceNotFoundException, MBeanRegistrationException, MalformedObjectNameException,
      NullPointerException {
    Region regionObject = new Region(reg);

    mbs.unregisterMBean(new ObjectName(Region.OBJECT_NAME + ",name=/" + reg));

    for (String member : regionObject.getMembers()) {
      mbs.unregisterMBean(new ObjectName(
          PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_REGION + regionObject.getFullPath()
              + PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_MEMBER + member));
    }
  }

  private void addServerMBean(String server)
      throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException,
      MalformedObjectNameException, NullPointerException {
    mbs.registerMBean(new ServerObject(server), new ObjectName(ServerObject.OBJECT_NAME));
  }

  private void removeServerMBean()
      throws InstanceNotFoundException, MBeanRegistrationException,
      MalformedObjectNameException, NullPointerException {
    mbs.unregisterMBean(new ObjectName(ServerObject.OBJECT_NAME));
  }

  private String[] getArrayProperty(JMXProperties props, String propName) {
    String propVal = props.getProperty(propName, "");
    return propVal.split(" ");
  }

  public static Server createServer(int jmxPort, String properties, String jsonAuthFile)
      throws Exception {
    return new Server(jmxPort, properties, jsonAuthFile);
  }
}
