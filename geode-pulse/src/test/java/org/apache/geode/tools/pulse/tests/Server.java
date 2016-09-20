/*
 *
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
 *
 */
package org.apache.geode.tools.pulse.tests;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.security.templates.SampleSecurityManager;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.realm.Realm;

import org.apache.geode.internal.security.shiro.CustomAuthRealm;
import org.apache.geode.internal.security.shiro.JMXShiroAuthenticator;
import org.apache.geode.management.internal.security.AccessControlMBean;
import org.apache.geode.management.internal.security.MBeanServerWrapper;
import org.apache.geode.management.internal.security.ResourceConstants;

public class Server {

  private static final String DEFAULT_HOST = "127.0.0.1"; //"localhost"
  private static final int DEFAULT_PORT = 9999;
  private final JMXServiceURL url;
  private MBeanServer mbs;
  private JMXConnectorServer cs;
  private String propFile = null;

  public Server(int port, String properties, String jsonAuthFile) throws Exception {
    this.propFile = properties;
    mbs = ManagementFactory.getPlatformMBeanServer();
    url = new JMXServiceURL(formJMXServiceURLString(DEFAULT_HOST, port));

    // Load the beans first, otherwise we get access denied
    loadMBeans();

    if (jsonAuthFile != null) {
      System.setProperty("spring.profiles.active", "pulse.authentication.gemfire");

      Map<String, Object> env = new HashMap<String, Object>();

      // set up Shiro Security Manager
      Properties securityProperties = new Properties();
      securityProperties.setProperty(SampleSecurityManager.SECURITY_JSON, jsonAuthFile);
      Realm realm = new CustomAuthRealm(SampleSecurityManager.class.getName(), securityProperties);
      SecurityManager securityManager = new DefaultSecurityManager(realm);
      SecurityUtils.setSecurityManager(securityManager);

      // register the AccessControll bean
      AccessControlMBean acc = new AccessControlMBean();
      ObjectName accessControlMBeanON = new ObjectName(ResourceConstants.OBJECT_NAME_ACCESSCONTROL);
      MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
      platformMBeanServer.registerMBean(acc, accessControlMBeanON);

      // wire in the authenticator and authorizaton
      JMXShiroAuthenticator interceptor = new JMXShiroAuthenticator();
      env.put(JMXConnectorServer.AUTHENTICATOR, interceptor);
      cs = JMXConnectorServerFactory.newJMXConnectorServer(url, env, mbs);
      cs.setMBeanServerForwarder(new MBeanServerWrapper());

      //set up the AccessControlMXBean

    } else {
      System.setProperty("spring.profiles.active", "pulse.authentication.default");
      cs = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
    }

    try {
      java.rmi.registry.LocateRegistry.createRegistry(port);
      System.out.println("RMI registry ready.");
    } catch (Exception e) {
      System.out.println("Exception starting RMI registry:");
      e.printStackTrace();
    }

    cs.start();
  }

  private String formJMXServiceURLString(String host, int port) throws UnknownHostException {
    String jmxSerURL = "";

    InetAddress inetAddr = InetAddress.getByName(host);
    if (inetAddr instanceof Inet4Address) {
      // Create jmx service url for IPv4 address
      jmxSerURL = "service:jmx:rmi://" + host + "/jndi/rmi://" + host + ":" + port + "/jmxrmi";
    } else if (inetAddr instanceof Inet6Address) {
      // Create jmx service url for IPv6 address
      jmxSerURL = "service:jmx:rmi://[" + host + "]/jndi/rmi://[" + host + "]:" + port + "/jmxrmi";
    }

    return jmxSerURL;
  }

  public void stop() {
    try {
      cs.stop();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private synchronized void loadMBeans() {
    JMXProperties props = JMXProperties.getInstance();
    try {
      props.load(propFile);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Add servers
    String[] servers = getArrayProperty(props, "servers");
    for (String server : servers) {
      try {
        addServerMBean(server);
      } catch (InstanceAlreadyExistsException e) {
        e.printStackTrace();
      } catch (MBeanRegistrationException e) {
        e.printStackTrace();
      } catch (NotCompliantMBeanException e) {
        e.printStackTrace();
      } catch (MalformedObjectNameException e) {
        e.printStackTrace();
      } catch (NullPointerException e) {
        e.printStackTrace();
      }
    }

    // Add members
    String[] members = getArrayProperty(props, "members");
    for (String m : members) {
      try {
        addMemberMBean(m);
      } catch (InstanceAlreadyExistsException e) {
        e.printStackTrace();
      } catch (MBeanRegistrationException e) {
        e.printStackTrace();
      } catch (NotCompliantMBeanException e) {
        e.printStackTrace();
      } catch (MalformedObjectNameException e) {
        e.printStackTrace();
      } catch (NullPointerException e) {
        e.printStackTrace();
      }
    }

    // Add regions
    String[] regions = getArrayProperty(props, "regions");
    for (String reg : regions) {
      try {
        addRegionMBean(reg);
      } catch (InstanceAlreadyExistsException e) {
        e.printStackTrace();
      } catch (MBeanRegistrationException e) {
        e.printStackTrace();
      } catch (NotCompliantMBeanException e) {
        e.printStackTrace();
      } catch (MalformedObjectNameException e) {
        e.printStackTrace();
      } catch (NullPointerException e) {
        e.printStackTrace();
      }
    }
  }

  private void addMemberMBean(
      String m) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException, NullPointerException {
    Member m1 = new Member(m);
    mbs.registerMBean(m1, new ObjectName(Member.OBJECT_NAME + ",member=" + m));
  }

  // For GemFire XD
  private void addGemFireXDMemberMBean(
      String xdm) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException, NullPointerException {
    GemFireXDMember xdmo = new GemFireXDMember(xdm);
    mbs.registerMBean(xdmo, new ObjectName(GemFireXDMember.OBJECT_NAME + ",member=" + xdm));
  }

  private void addRegionMBean(
      String reg) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException, NullPointerException {
    Region regionObject = new Region(reg);
    mbs.registerMBean(regionObject, new ObjectName(Region.OBJECT_NAME + ",name=/" + reg));

    for (String member : regionObject.getMembers()) {
      RegionOnMember regionOnMemberObject = new RegionOnMember(regionObject.getFullPath(), member);
      mbs.registerMBean(regionOnMemberObject, new ObjectName(
          PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_REGION + regionObject.getFullPath() + PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_MEMBER + member));
    }
  }

  private void addServerMBean(
      String server) throws InstanceAlreadyExistsException, MBeanRegistrationException, NotCompliantMBeanException, MalformedObjectNameException, NullPointerException {
    ServerObject so = new ServerObject(server);
    mbs.registerMBean(so, new ObjectName(ServerObject.OBJECT_NAME));
  }

  private String[] getArrayProperty(JMXProperties props, String propName) {
    String propVal = props.getProperty(propName, "");
    return propVal.split(" ");
  }

  public static Server createServer(int port, String properties, String jsonAuthFile)
      throws MalformedObjectNameException {
    Server s = null;
    try {
      s = new Server(port, properties, jsonAuthFile);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    return s;
  }
}
