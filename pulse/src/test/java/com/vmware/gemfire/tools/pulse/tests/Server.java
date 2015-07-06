/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

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

    // Add GemFireXD Cluster
    String[] gemFireXDClusters = getArrayProperty(props, "gemfirexdclusters");
    for (String xdCluster : gemFireXDClusters) {
      try {
        addGemFireXDClusterMbean(xdCluster);
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

    // Add GemFireXD members
    String[] gemfirexdmember = getArrayProperty(props, "gemfirexdmembers");
    for (String xdm : gemfirexdmember) {
      try {
        addGemFireXDMemberMBean(xdm);
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

    // Add GemFireXD Aggregate Tables
    String[] aggTables = getArrayProperty(props, "tables");
    for (String aggTable : aggTables) {
      try {
        addGemFireXDAggregateTableMBean(aggTable);
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

    // Add aggregate queries
    System.out.println("loading aggregate queries mbeans ...");
    String[] aggregatequeries = getArrayProperty(props, "aggregatequeries");
    for (String aggregateq : aggregatequeries) {
      try {
        System.out.println("adding mbean .. " + aggregateq);
        addAggregateQueryMBean(aggregateq);
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
    System.out.println("Finished registering # mbeans .. " + aggregatequeries.length);
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
    System.out.println("Region Full path : " + regionObject.getFullPath());

    for (String member : regionObject.getMembers()) {
      System.out.println("Adding Region on Member MBean for Region: " + reg + ", Member: " + member);
      RegionOnMember regionOnMemberObject = new RegionOnMember(regionObject.getFullPath());
      mbs.registerMBean(regionOnMemberObject, new ObjectName(
          "GemFire:service=Region,name=/"+ regionObject.getFullPath() + ",type=Member,member=" + member));
      System.out.println("Region on member properties retrieved : entryCount=" + regionOnMemberObject.getEntryCount() +",entrySize="
          + regionOnMemberObject.getEntrySize() + ",localMaxMemory=" +regionOnMemberObject.getLocalMaxMemory());
    }
  }

  // For GemFire XD
  private void addGemFireXDAggregateTableMBean(String aggTable)
      throws InstanceAlreadyExistsException, MBeanRegistrationException,
      NotCompliantMBeanException, MalformedObjectNameException,
      NullPointerException {
    GemFireXDAggregateTable tableObject = new GemFireXDAggregateTable(aggTable);
    mbs.registerMBean(tableObject, new ObjectName(GemFireXDAggregateTable.OBJECT_NAME + aggTable));
  }

  private void addServerMBean(String server)
      throws InstanceAlreadyExistsException, MBeanRegistrationException,
      NotCompliantMBeanException, MalformedObjectNameException,
      NullPointerException {
    ServerObject so = new ServerObject(server);
    mbs.registerMBean(so, new ObjectName(ServerObject.OBJECT_NAME));
  }

  // For GemFire XD
  private void addGemFireXDClusterMbean(String cluster)
      throws InstanceAlreadyExistsException, MBeanRegistrationException,
      NotCompliantMBeanException, MalformedObjectNameException,
      NullPointerException {
    GemFireXDCluster xdco = new GemFireXDCluster(cluster);
    mbs.registerMBean(xdco, new ObjectName(GemFireXDCluster.OBJECT_NAME));
  }

  private void addAggregateQueryMBean(String aggregateq)
      throws InstanceAlreadyExistsException, MBeanRegistrationException,
      NotCompliantMBeanException, MalformedObjectNameException,
      NullPointerException {
    AggregateStatement aggregatestatement = new AggregateStatement(aggregateq);
    System.out.println("created statement mbean ..." + aggregatestatement + ", query defn = " + ObjectName.quote(aggregatestatement.getQueryDefinition()));
    System.out.println("qnRespDeSerTime ...qnRespDeSerTime = " + aggregatestatement.getQNRespDeSerTime());
    Object returnObj = mbs.registerMBean(aggregatestatement, new ObjectName(AggregateStatementMBean.OBJECT_NAME + ",name=" + ObjectName.quote(aggregatestatement.getQueryDefinition())));
    System.out.println("registered mbean ..." + returnObj );
  }

  private String[] getArrayProperty(JMXProperties props, String propName) {
    String propVal = props.getProperty(propName, "");
    return propVal.split(" ");
  }

  private void unregisterAll() {
    Set<ObjectName> thisSet = mbs.queryNames(null, null);
    for (ObjectName objectName : thisSet) {
      System.out.println("Removing ..." + objectName.getCanonicalName());

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
