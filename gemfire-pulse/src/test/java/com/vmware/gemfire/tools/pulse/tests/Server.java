/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package com.vmware.gemfire.tools.pulse.tests;

import com.vmware.gemfire.tools.pulse.internal.data.PulseConstants;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

public class Server {
  private static final String DEFAULT_HOST = "127.0.0.1"; //"localhost"
  private static final int DEFAULT_PORT = 9999;
  private final MBeanServer mbs;
  private final JMXServiceURL url;
  private final JMXConnectorServer cs;
  private String propFile = null;

  public Server(int port, String properties) throws IOException {
    try {
      java.rmi.registry.LocateRegistry.createRegistry(port);
      System.out.println("RMI registry ready.");
    } catch (Exception e) {
      System.out.println("Exception starting RMI registry:");
      e.printStackTrace();
    }

    this.propFile = properties;
    mbs = MBeanServerFactory.createMBeanServer();
    url = new JMXServiceURL(formJMXServiceURLString(DEFAULT_HOST, "" + port));
    cs = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);

    cs.start();

    loadMBeans();
  }

  private String formJMXServiceURLString(String host, String port)
      throws UnknownHostException {
    /*
     * String jmxSerURL = "service:jmx:rmi://" + serverName + "/jndi/rmi://" +
     * serverName + ":" + port + "/jmxrmi";
     */
    String jmxSerURL = "";

    InetAddress inetAddr = InetAddress.getByName(host);
    if (inetAddr instanceof Inet4Address) {
      // Create jmx service url for IPv4 address
      jmxSerURL = "service:jmx:rmi://" + host + "/jndi/rmi://" + host + ":"
          + port + "/jmxrmi";
    } else if (inetAddr instanceof Inet6Address) {
      // Create jmx service url for IPv6 address
      jmxSerURL = "service:jmx:rmi://[" + host + "]/jndi/rmi://[" + host + "]:"
          + port + "/jmxrmi";
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
    unregisterAll();

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

  private void addMemberMBean(String m) throws InstanceAlreadyExistsException,
      MBeanRegistrationException, NotCompliantMBeanException,
      MalformedObjectNameException, NullPointerException {
    Member m1 = new Member(m);
    mbs.registerMBean(m1, new ObjectName(Member.OBJECT_NAME + ",member=" + m));
  }

  // For GemFire XD
  private void addGemFireXDMemberMBean(String xdm)
      throws InstanceAlreadyExistsException, MBeanRegistrationException,
      NotCompliantMBeanException, MalformedObjectNameException,
      NullPointerException {
    GemFireXDMember xdmo = new GemFireXDMember(xdm);
    mbs.registerMBean(xdmo, new ObjectName(GemFireXDMember.OBJECT_NAME
        + ",member=" + xdm));
  }

  private void addRegionMBean(String reg)
      throws InstanceAlreadyExistsException, MBeanRegistrationException,
      NotCompliantMBeanException, MalformedObjectNameException,
      NullPointerException {
    Region regionObject = new Region(reg);
    mbs.registerMBean(regionObject, new ObjectName(Region.OBJECT_NAME + ",name=/" + reg));

    for (String member : regionObject.getMembers()) {
      RegionOnMember regionOnMemberObject = new RegionOnMember(regionObject.getFullPath(), member);
      mbs.registerMBean(regionOnMemberObject, new ObjectName(
              PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_REGION + regionObject.getFullPath() + PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_MEMBER + member));
    }
  }

  private void addServerMBean(String server)
      throws InstanceAlreadyExistsException, MBeanRegistrationException,
      NotCompliantMBeanException, MalformedObjectNameException,
      NullPointerException {
    ServerObject so = new ServerObject(server);
    mbs.registerMBean(so, new ObjectName(ServerObject.OBJECT_NAME));
  }

  private String[] getArrayProperty(JMXProperties props, String propName) {
    String propVal = props.getProperty(propName, "");
    return propVal.split(" ");
  }

  private void unregisterAll() {
    Set<ObjectName> thisSet = mbs.queryNames(null, null);
    for (ObjectName objectName : thisSet) {
//      System.out.println("Removing ..." + objectName.getCanonicalName());

      /*try {
        mbs.unregisterMBean(objectName);
      } catch (MBeanRegistrationException e) {
        e.printStackTrace();
      } catch (InstanceNotFoundException e) {
        e.printStackTrace();
      }*/
    }
  }

  public static void main(String[] args) throws MalformedObjectNameException,
      NullPointerException {
    int port = DEFAULT_PORT;
    String props = null;
    if (args.length >= 2) {
      try {
        port = Integer.parseInt(args[0]);
        props = args[1];

      } catch (NumberFormatException nfe) {
        port = DEFAULT_PORT;
        props = null;
        nfe.printStackTrace();
      }
    }

    createServer(port, props);
  }

  public static Server createServer(int port, String properties)
      throws MalformedObjectNameException {
    Server s = null;
    try {
      s = new Server(port, properties);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    return s;
  }
}
