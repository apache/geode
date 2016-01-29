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

package com.vmware.gemfire.tools.pulse.internal.data;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ResourceBundle;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.rmi.ssl.SslRMIClientSocketFactory;

import com.vmware.gemfire.tools.pulse.internal.controllers.PulseController;
import com.vmware.gemfire.tools.pulse.internal.data.JmxManagerFinder.JmxManagerInfo;
import com.vmware.gemfire.tools.pulse.internal.json.JSONException;
import com.vmware.gemfire.tools.pulse.internal.json.JSONObject;
import com.vmware.gemfire.tools.pulse.internal.log.PulseLogWriter;
import com.vmware.gemfire.tools.pulse.internal.util.StringUtils;

/**
 * Class JMXDataUpdater Class used for creating JMX connection and getting all
 * the required MBeans
 *
 * @author Anand Hariharan
 *
 * @since version 7.0.Beta 2012-09-23
 */
public class JMXDataUpdater implements IClusterUpdater, NotificationListener {

  private final PulseLogWriter LOGGER = PulseLogWriter.getLogger();
  private final ResourceBundle resourceBundle = Repository.get()
      .getResourceBundle();

  private JMXConnector conn = null;
  private MBeanServerConnection mbs;
  private final String serverName;
  private final String port;
  private final String userName;
  private final String userPassword;
  private Boolean isAddedNotiListner = false;
  private final Cluster cluster;

  // MBean object names
  private ObjectName MBEAN_OBJECT_NAME_SYSTEM_DISTRIBUTED;
  private ObjectName MBEAN_OBJECT_NAME_REGION_DISTRIBUTED;
  private ObjectName MBEAN_OBJECT_NAME_MEMBER;
  private ObjectName MBEAN_OBJECT_NAME_MEMBER_MANAGER;
  private ObjectName MBEAN_OBJECT_NAME_STATEMENT_DISTRIBUTED;
  private ObjectName MBEAN_OBJECT_NAME_TABLE_AGGREGATE;

  private Set<ObjectName> systemMBeans = null;

  private final String opSignature[] = { String.class.getName(),
      String.class.getName(), int.class.getName() };

  /**
   * constructor used for creating JMX connection
   */
  public JMXDataUpdater(String server, String port, Cluster cluster) {
    this.serverName = server;
    this.port = port;
    this.userName = cluster.getJmxUserName();
    this.userPassword = cluster.getJmxUserPassword();
    this.cluster = cluster;

    try {
      // Initialize MBean object names
      this.MBEAN_OBJECT_NAME_SYSTEM_DISTRIBUTED = new ObjectName(
          PulseConstants.OBJECT_NAME_SYSTEM_DISTRIBUTED);
      this.MBEAN_OBJECT_NAME_REGION_DISTRIBUTED = new ObjectName(
          PulseConstants.OBJECT_NAME_REGION_DISTRIBUTED);
      this.MBEAN_OBJECT_NAME_MEMBER_MANAGER = new ObjectName(
          PulseConstants.OBJECT_NAME_MEMBER_MANAGER);
      this.MBEAN_OBJECT_NAME_MEMBER = new ObjectName(
          PulseConstants.OBJECT_NAME_MEMBER);
      this.MBEAN_OBJECT_NAME_STATEMENT_DISTRIBUTED = new ObjectName(
          PulseConstants.OBJECT_NAME_STATEMENT_DISTRIBUTED);

      // For SQLFire
      if (PulseConstants.PRODUCT_NAME_SQLFIRE.equalsIgnoreCase(PulseController
          .getPulseProductSupport())) {
        this.MBEAN_OBJECT_NAME_TABLE_AGGREGATE = new ObjectName(
            PulseConstants.OBJECT_NAME_TABLE_AGGREGATE);
      }

    } catch (MalformedObjectNameException e) {
      if (LOGGER.severeEnabled()) {
        LOGGER.severe(e.getMessage(), e);
      }
    } catch (NullPointerException e) {
      if (LOGGER.severeEnabled()) {
        LOGGER.severe(e.getMessage(), e);
      }
    }

  }

  private JMXConnector getJMXConnection() {
    JMXConnector connection = null;
    // Reference to repository
    Repository repository = Repository.get();
    try {

      String jmxSerURL = "";

      if (LOGGER.infoEnabled()) {
        LOGGER.info(resourceBundle.getString("LOG_MSG_USE_LOCATOR_VALUE") + ":"
            + repository.getJmxUseLocator());
      }

      if (repository.getJmxUseLocator()) {

        String locatorHost = repository.getJmxHost();
        int locatorPort = Integer.parseInt(repository.getJmxPort());

        if (LOGGER.infoEnabled()) {
          LOGGER.info(resourceBundle.getString("LOG_MSG_HOST") + " : "
              + locatorHost + " & " + resourceBundle.getString("LOG_MSG_PORT")
              + " : " + locatorPort);
        }

        InetAddress inetAddr = InetAddress.getByName(locatorHost);

        if ((inetAddr instanceof Inet4Address)
            || (inetAddr instanceof Inet6Address)) {

          if (inetAddr instanceof Inet4Address) {
            // Locator has IPv4 Address
            if (LOGGER.infoEnabled()) {
              LOGGER.info(resourceBundle
                  .getString("LOG_MSG_LOCATOR_IPV4_ADDRESS")
                  + " - "
                  + inetAddr.toString());
            }
          } else {
            // Locator has IPv6 Address
            if (LOGGER.infoEnabled()) {
              LOGGER.info(resourceBundle
                  .getString("LOG_MSG_LOCATOR_IPV6_ADDRESS")
                  + " - "
                  + inetAddr.toString());
            }
          }

          JmxManagerInfo jmxManagerInfo = JmxManagerFinder
              .askLocatorForJmxManager(inetAddr, locatorPort, 15000,
                  repository.isUseSSLLocator());

          if (jmxManagerInfo.port == 0) {
            if (LOGGER.infoEnabled()) {
              LOGGER.info(resourceBundle
                  .getString("LOG_MSG_LOCATOR_COULD_NOT_FIND_MANAGER"));
            }
          } else {
            if (LOGGER.infoEnabled()) {
              LOGGER.info(resourceBundle
                  .getString("LOG_MSG_LOCATOR_FOUND_MANAGER")
                  + " : "
                  + resourceBundle.getString("LOG_MSG_HOST")
                  + " : "
                  + jmxManagerInfo.host
                  + " & "
                  + resourceBundle.getString("LOG_MSG_PORT")
                  + " : "
                  + jmxManagerInfo.port
                  + (jmxManagerInfo.ssl ? resourceBundle
                      .getString("LOG_MSG_WITH_SSL") : resourceBundle
                      .getString("LOG_MSG_WITHOUT_SSL")));
            }

            jmxSerURL = formJMXServiceURLString(jmxManagerInfo.host,
                String.valueOf(jmxManagerInfo.port));
          }

        } /*
           * else if (inetAddr instanceof Inet6Address) { // Locator has IPv6
           * Address if(LOGGER.infoEnabled()){
           * LOGGER.info(resourceBundle.getString
           * ("LOG_MSG_LOCATOR_IPV6_ADDRESS")); } // update message to display
           * on UI cluster.setConnectionErrorMsg(resourceBundle.getString(
           * "LOG_MSG_JMX_CONNECTION_IPv6_ADDRESS"));
           *
           * }
           */else {
          // Locator has Invalid locator Address
          if (LOGGER.infoEnabled()) {
            LOGGER
                .info(resourceBundle.getString("LOG_MSG_LOCATOR_BAD_ADDRESS"));
          }
          // update message to display on UI
          cluster.setConnectionErrorMsg(resourceBundle
              .getString("LOG_MSG_JMX_CONNECTION_BAD_ADDRESS"));

        }

      } else {
        if (LOGGER.infoEnabled()) {
          LOGGER.info(resourceBundle.getString("LOG_MSG_HOST") + " : "
              + this.serverName + " & "
              + resourceBundle.getString("LOG_MSG_PORT") + " : " + this.port);
        }
        jmxSerURL = formJMXServiceURLString(this.serverName, this.port);
      }

      if (StringUtils.isNotNullNotEmptyNotWhiteSpace(jmxSerURL)) {
        JMXServiceURL url = new JMXServiceURL(jmxSerURL);

        // String[] creds = {"controlRole", "R&D"};
        String[] creds = { this.userName, this.userPassword };
        Map<String, Object> env = new HashMap<String, Object>();

        env.put(JMXConnector.CREDENTIALS, creds);

        if (repository.isUseSSLManager()) {
          // use ssl to connect
          env.put("com.sun.jndi.rmi.factory.socket",
              new SslRMIClientSocketFactory());
        }

        connection = JMXConnectorFactory.connect(url, env);

        // Register Pulse URL if not already present in the JMX Manager
        registerPulseUrlToManager(connection);
      }
    } catch (Exception e) {
      if (e instanceof UnknownHostException) {
        // update message to display on UI
        cluster.setConnectionErrorMsg(resourceBundle
            .getString("LOG_MSG_JMX_CONNECTION_UNKNOWN_HOST"));
      }

      // write errors
      StringWriter swBuffer = new StringWriter();
      PrintWriter prtWriter = new PrintWriter(swBuffer);
      e.printStackTrace(prtWriter);
      LOGGER.severe("Exception Details : " + swBuffer.toString() + "\n");
      if (this.conn != null) {
        try {
          this.conn.close();
        } catch (Exception e1) {
          LOGGER.severe("Error closing JMX connection " + swBuffer.toString()
              + "\n");
        }
        this.conn = null;
      }
    }

    return connection;
  }

  private String formJMXServiceURLString(String host, String port)
      throws UnknownHostException {
    /*
     * String jmxSerURL = "service:jmx:rmi://" + serverName + "/jndi/rmi://" +
     * serverName + ":" + port + "/jmxrmi";
     */
    String jmxSerURL = "";
    if (host.equalsIgnoreCase("localhost")) {
      // Create jmx service url for 'localhost'
      jmxSerURL = "service:jmx:rmi://" + host + "/jndi/rmi://" + host + ":"
          + port + "/jmxrmi";
    } else {
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
    }

    return jmxSerURL;
  }

  // Method registers Pulse URL if not already present in the JMX Manager
  private void registerPulseUrlToManager(JMXConnector connection)
      throws IOException, AttributeNotFoundException,
      InstanceNotFoundException, MBeanException, ReflectionException,
      MalformedObjectNameException, InvalidAttributeValueException {
    if (LOGGER.infoEnabled()) {
      LOGGER.info(resourceBundle
          .getString("LOG_MSG_REGISTERING_APP_URL_TO_MANAGER"));
    }

    // Reference to repository
    Repository repository = Repository.get();

    // Register Pulse URL if not already present in the JMX Manager
    if (connection != null) {
      MBeanServerConnection mbsc = connection.getMBeanServerConnection();

      Set<ObjectName> mbeans = mbsc.queryNames(
          this.MBEAN_OBJECT_NAME_MEMBER_MANAGER, null);

      for (ObjectName mbeanName : mbeans) {
        String presentUrl = (String) mbsc.getAttribute(mbeanName,
            PulseConstants.MBEAN_MANAGER_ATTRIBUTE_PULSEURL);
        String pulseWebAppUrl = repository.getPulseWebAppUrl();
        if (pulseWebAppUrl != null
            && (presentUrl == null || !pulseWebAppUrl.equals(presentUrl))) {
          if (LOGGER.fineEnabled()) {
            LOGGER.fine(resourceBundle
                .getString("LOG_MSG_SETTING_APP_URL_TO_MANAGER"));
          }
          Attribute pulseUrlAttr = new Attribute(
              PulseConstants.MBEAN_MANAGER_ATTRIBUTE_PULSEURL, pulseWebAppUrl);
          mbsc.setAttribute(mbeanName, pulseUrlAttr);
        } else {
          if (LOGGER.fineEnabled()) {
            LOGGER.fine(resourceBundle
                .getString("LOG_MSG_APP_URL_ALREADY_PRESENT_IN_MANAGER"));
          }
        }
      }
    }
  }

  private boolean isConnected() {
    // Reference to repository
    Repository repository = Repository.get();
    if (repository.getIsEmbeddedMode()) {
      if (this.mbs == null) {
        this.mbs = ManagementFactory.getPlatformMBeanServer();
        cluster.setConnectedFlag(true);
      }
    } else {
      try {
        if (this.conn == null) {
          cluster.setConnectedFlag(false);
          cluster.setConnectionErrorMsg(resourceBundle
              .getString("LOG_MSG_JMX_CONNECTION_NOT_FOUND")
              + " "
              + resourceBundle.getString("LOG_MSG_JMX_GETTING_NEW_CONNECTION"));
          if (LOGGER.fineEnabled()) {
            LOGGER.fine(resourceBundle
                .getString("LOG_MSG_JMX_CONNECTION_NOT_FOUND")
                + " "
                + resourceBundle.getString("LOG_MSG_JMX_GET_NEW_CONNECTION"));
          }
          this.conn = getJMXConnection();
          if (this.conn != null) {
            this.mbs = this.conn.getMBeanServerConnection();
            cluster.setConnectedFlag(true);
          } else {
            if (LOGGER.infoEnabled()) {
              LOGGER.info(resourceBundle
                  .getString("LOG_MSG_JMX_CONNECTION_NOT_FOUND"));
            }
            return false;
          }
        } else {
          if (LOGGER.fineEnabled()) {
            LOGGER.fine(resourceBundle
                .getString("LOG_MSG_JMX_CONNECTION_IS_AVAILABLE"));
          }
          cluster.setConnectedFlag(true);
          if (this.mbs == null) {
            this.mbs = this.conn.getMBeanServerConnection();
          }
        }
      } catch (Exception e) {
        this.mbs = null;
        if (this.conn != null) {
          try {
            this.conn.close();
          } catch (Exception e1) {
            LOGGER.severe(e);
          }
        }
        this.conn = null;
        return false;
      }
    }

    return true;
  }

  /**
   * function used for updating Cluster Data.
   */
  @Override
  public boolean updateData() {
    try {
      if (!this.isConnected()) {
        return false;
      }

      // deleted Members
      cluster.getDeletedMembers().clear();
      for (Entry<String, Cluster.Member> memberSet : cluster.getMembersHMap()
          .entrySet()) {
        cluster.getDeletedMembers().add(memberSet.getKey());
      }

      // Deleted Regions
      cluster.getDeletedRegions().clear();
      for (Cluster.Region region : cluster.getClusterRegions().values()) {
        cluster.getDeletedRegions().add(region.getFullPath());
      }

      // try {

      // Cluster
      this.systemMBeans = this.mbs.queryNames(
          this.MBEAN_OBJECT_NAME_SYSTEM_DISTRIBUTED, null);
      for (ObjectName sysMBean : this.systemMBeans) {
        updateClusterSystem(sysMBean);
      }

      // Cluster Regions/Tables
      Set<ObjectName> regionMBeans = this.mbs.queryNames(
          this.MBEAN_OBJECT_NAME_REGION_DISTRIBUTED, null);

      Set<ObjectName> tableMBeans = this.mbs.queryNames(
          this.MBEAN_OBJECT_NAME_TABLE_AGGREGATE, null);

      if (PulseConstants.PRODUCT_NAME_SQLFIRE.equalsIgnoreCase(PulseController
          .getPulseProductSupport())) {
        // For SQLfire
        for (ObjectName tableMBean : tableMBeans) {
          String regNameFromTable = StringUtils
              .getRegionNameFromTableName(tableMBean.getKeyProperty("table"));
          for (ObjectName regionMBean : regionMBeans) {
            String regionName = regionMBean.getKeyProperty("name");
            if (regNameFromTable.equals(regionName)) {
              updateClusterRegion(regionMBean);
              // Increment cluster region count
              cluster.setTotalRegionCount(cluster.getTotalRegionCount() + 1);
              break;
            }
          }
        }
      } else {
        // For Gemfire
        for (ObjectName regMBean : regionMBeans) {
          updateClusterRegion(regMBean);
        }
      }

      // Remove deleted regions from cluster's regions list
      for (Iterator<String> it = cluster.getDeletedRegions().iterator(); it
          .hasNext();) {
        cluster.removeClusterRegion(it.next());
      }

      // Cluster Members
      Set<ObjectName> memberMBeans = this.mbs.queryNames(
          this.MBEAN_OBJECT_NAME_MEMBER, null);
      for (ObjectName memMBean : memberMBeans) {
        // member regions
        if (memMBean.getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_SERVICE) != null) {
          if (memMBean
              .getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_SERVICE)
              .equals(PulseConstants.MBEAN_KEY_PROPERTY_SERVICE_VALUE_REGION)) {

            if (PulseConstants.PRODUCT_NAME_SQLFIRE
                .equalsIgnoreCase(PulseController.getPulseProductSupport())) {
              // For SQLfire
              for (ObjectName tableMBean : tableMBeans) {
                String regNameFromTable = StringUtils
                    .getRegionNameFromTableName(tableMBean
                        .getKeyProperty("table"));
                String regionName = memMBean.getKeyProperty("name");
                if (regNameFromTable.equals(regionName)) {
                  updateMemberRegion(memMBean);
                  break;
                }
              }
            } else {
              // For Gemfire
              updateMemberRegion(memMBean);
            }

          } else if (memMBean.getKeyProperty(
              PulseConstants.MBEAN_KEY_PROPERTY_SERVICE).equals(
              PulseConstants.MBEAN_KEY_PROPERTY_SERVICE_VALUE_CACHESERVER)) {
            updateMemberClient(memMBean);
          }
          // Gateway Receiver Attributes
          else if (memMBean.getKeyProperty(
              PulseConstants.MBEAN_KEY_PROPERTY_SERVICE).equals(
              PulseConstants.MBEAN_KEY_PROPERTY_SERVICE_VALUE_GATEWAYRECEIVER)) {
            updateGatewayReceiver(memMBean);
          } else if (memMBean.getKeyProperty(
              PulseConstants.MBEAN_KEY_PROPERTY_SERVICE).equals(
              PulseConstants.MBEAN_KEY_PROPERTY_SERVICE_VALUE_GATEWAYSENDER)) {
              updateGatewaySender(memMBean);
          } else if(memMBean.getKeyProperty(
              PulseConstants.MBEAN_KEY_PROPERTY_SERVICE).equals(
              PulseConstants.MBEAN_KEY_PROPERTY_SERVICE_VALUE_ASYNCEVENTQUEUE)){

              // AsyncEventQueue
              updateAsyncEventQueue(memMBean);
          }
        } else {
          // Cluster Member
          updateClusterMember(memMBean);
        }
      }

      // Cluster Query Statistics
      Set<ObjectName> statementObjectNames = this.mbs.queryNames(
          this.MBEAN_OBJECT_NAME_STATEMENT_DISTRIBUTED, null);
      //LOGGER.info("statementObjectNames = " + statementObjectNames);
      for (ObjectName stmtObjectName : statementObjectNames) {
        //LOGGER.info("stmtObjectName = " + stmtObjectName);
        updateClusterStatement(stmtObjectName);
      }
    } catch (IOException ioe) {

      // write errors
      StringWriter swBuffer = new StringWriter();
      PrintWriter prtWriter = new PrintWriter(swBuffer);
      ioe.printStackTrace(prtWriter);
      LOGGER.severe("IOException Details : " + swBuffer.toString() + "\n");
      this.mbs = null;
      if (this.conn != null) {
        try {
          this.conn.close();
        } catch (IOException e1) {
          LOGGER.severe("Error closing JMX connection " + swBuffer.toString()
              + "\n");
        }
      }

      return false;
    }

    // If there were members deleted, remove them from the membersList &
    // physicalToMember.
    Iterator<String> iterator = cluster.getDeletedMembers().iterator();
    while (iterator.hasNext()) {
      String memberKey = iterator.next();
      if (cluster.getMembersHMap().containsKey(memberKey)) {
        Cluster.Member member = cluster.getMembersHMap().get(memberKey);
        List<Cluster.Member> memberArrList = cluster.getPhysicalToMember().get(
            member.getHost());
        if (memberArrList != null) {
          if (memberArrList.contains(member)) {
            String host = member.getHost();
            cluster.getPhysicalToMember().get(member.getHost()).remove(member);

            if (cluster.getPhysicalToMember().get(member.getHost()).size() == 0) {
              cluster.getPhysicalToMember().remove(host);
            }
          }
        }
        cluster.getMembersHMap().remove(memberKey);
      }

    }

    return true;
  }

  /**
   * function used to get attribute values of Cluster System and map them to
   * cluster vo
   *
   * @param mbeanName
   *          Cluster System MBean
   * @throws IOException
   *
   */
  private void updateClusterSystem(ObjectName mbeanName) throws IOException {
    try {
      if (!this.isAddedNotiListner) {
        this.mbs.addNotificationListener(mbeanName, this, null, new Object());
        this.isAddedNotiListner = true;
      }

      if (PulseConstants.PRODUCT_NAME_SQLFIRE.equalsIgnoreCase(PulseController
          .getPulseProductSupport())) {
        // Reset to zero
        cluster.setServerCount(0);
        cluster.setTotalRegionCount(0);
      } else {
        String[] serverCnt = (String[]) (this.mbs.invoke(mbeanName,
            PulseConstants.MBEAN_OPERATION_LISTSERVERS, null, null));
        cluster.setServerCount(serverCnt.length);
      }

      TabularData table = (TabularData) (this.mbs.invoke(mbeanName,
          PulseConstants.MBEAN_OPERATION_VIEWREMOTECLUSTERSTATUS, null, null));

      Collection<CompositeData> rows = (Collection<CompositeData>) table
          .values();
      cluster.getWanInformationObject().clear();
      for (CompositeData row : rows) {
        final Object key = row.get("key");
        final Object value = row.get("value");
        cluster.getWanInformationObject().put((String) key, (Boolean) value);
      }

      AttributeList attributeList = this.mbs.getAttributes(mbeanName,
          PulseConstants.CLUSTER_MBEAN_ATTRIBUTES);

      for (int i = 0; i < attributeList.size(); i++) {

        Attribute attribute = (Attribute) attributeList.get(i);

        if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_MEMBERCOUNT)) {
          cluster.setMemberCount(getIntegerAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_NUMCLIENTS)) {
          cluster.setClientConnectionCount(getIntegerAttribute(
              attribute.getValue(), attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_DISTRIBUTEDSYSTEMID)) {
          cluster.setClusterId(getIntegerAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_LOCATORCOUNT)) {
          cluster.setLocatorCount(getIntegerAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_NUMRUNNIGFUNCTION)) {
          try {
            cluster.setRunningFunctionCount(getIntegerAttribute(
                attribute.getValue(), attribute.getName()));
          } catch (Exception e) {
            cluster.setRunningFunctionCount(0);
            continue;
          }
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_REGISTEREDCQCOUNT)) {
          cluster.setRegisteredCQCount(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_NUMSUBSCRIPTIONS)) {
          cluster.setSubscriptionCount(getIntegerAttribute(
              attribute.getValue(), attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_NUMTXNCOMMITTED)) {
          cluster.setTxnCommittedCount(getIntegerAttribute(
              attribute.getValue(), attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_NUMTXNROLLBACK)) {
          cluster.setTxnRollbackCount(getIntegerAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_TOTALHEAPSIZE)) {
          cluster.setTotalHeapSize(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_USEDHEAPSIZE)) {
          try {
            cluster.setUsedHeapSize(getLongAttribute(attribute.getValue(),
                attribute.getName()));
          } catch (Exception e) {
            cluster.setUsedHeapSize((long) 0);
            continue;
          }
          cluster.getMemoryUsageTrend().add(cluster.getUsedHeapSize());
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_TOTALREGIONENTRYCOUNT)) {
          cluster.setTotalRegionEntryCount(getLongAttribute(
              attribute.getValue(), attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_CURRENTENTRYCOUNT)) {
          cluster.setCurrentQueryCount(getIntegerAttribute(
              attribute.getValue(), attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_TOTALDISKUSAGE)) {
          try {
            cluster.setTotalBytesOnDisk(getLongAttribute(attribute.getValue(),
                attribute.getName()));
          } catch (Exception e) {
            cluster.setTotalBytesOnDisk((long) 0);
            continue;
          }
          cluster.getTotalBytesOnDiskTrend().add(cluster.getTotalBytesOnDisk());
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_DISKWRITESRATE)) {
          cluster.setDiskWritesRate(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
          cluster.getThroughoutWritesTrend().add(cluster.getDiskWritesRate());
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_AVERAGEWRITES)) {
          try {
            cluster.setWritePerSec(getFloatAttribute(attribute.getValue(),
                attribute.getName()));
          } catch (Exception e) {
            cluster.setWritePerSec(0);
            continue;
          }
          cluster.getWritePerSecTrend().add(cluster.getWritePerSec());
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_AVERAGEREADS)) {
          try {
            cluster.setReadPerSec(getFloatAttribute(attribute.getValue(),
                attribute.getName()));
          } catch (Exception e) {
            cluster.setReadPerSec(0);
            continue;
          }
          cluster.getReadPerSecTrend().add(cluster.getReadPerSec());

        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_QUERYREQUESTRATE)) {
          cluster.setQueriesPerSec(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
          cluster.getQueriesPerSecTrend().add(cluster.getQueriesPerSec());

        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_DISKREADSRATE)) {
          cluster.setDiskReadsRate(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
          cluster.getThroughoutReadsTrend().add(cluster.getDiskReadsRate());
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_JVMPAUSES)) {
          long trendVal = determineCurrentJVMPauses(
              PulseConstants.JVM_PAUSES_TYPE_CLUSTER, "",
              getLongAttribute(attribute.getValue(), attribute.getName()));
          cluster.setGarbageCollectionCount(trendVal);
          cluster.getGarbageCollectionTrend().add(
              cluster.getGarbageCollectionCount());

        }

        // For SQLfire or Gemfire
        if (PulseConstants.PRODUCT_NAME_SQLFIRE
            .equalsIgnoreCase(PulseController.getPulseProductSupport())) {
          // For SQLfire
          // Do nothing
        } else {
          // For Gemfire
          if (attribute.getName().equals(
              PulseConstants.MBEAN_ATTRIBUTE_TOTALREGIONCOUNT)) {
            cluster.setTotalRegionCount(getIntegerAttribute(
                attribute.getValue(), attribute.getName()));
          }
        }

      }

      // SQLFIRE attributes
      if (PulseConstants.PRODUCT_NAME_SQLFIRE.equalsIgnoreCase(PulseController
          .getPulseProductSupport())) {

        try { // get sqlfire cluster mbean

          ObjectName sfMemberMbeansObjectName = new ObjectName(
              PulseConstants.OBJECT_NAME_SF_CLUSTER);

          Set<ObjectName> sfCluserMBeans = this.mbs.queryNames(
              sfMemberMbeansObjectName, null);

          for (ObjectName sfCluserMBean : sfCluserMBeans) {

            AttributeList attrList = this.mbs.getAttributes(sfCluserMBean,
                PulseConstants.SF_CLUSTER_MBEAN_ATTRIBUTES);

            for (int i = 0; i < attrList.size(); i++) {

              Attribute attribute = (Attribute) attrList.get(i);

              if (attribute.getName().equals(
                  PulseConstants.MBEAN_ATTRIBUTE_PROCEDURECALLSINPROGRESS)) {
                try {
                  cluster.setRunningFunctionCount(getIntegerAttribute(
                      attribute.getValue(), attribute.getName()));
                } catch (Exception e) {
                  cluster.setRunningFunctionCount(0);
                  continue;
                }
              } else if (attribute
                  .getName()
                  .equals(
                      PulseConstants.MBEAN_ATTRIBUTE_NETWORKSERVERCLIENTCONNECTIONSTATS)) {
                // set number of cluster's clients
                CompositeData nscConnStats = (CompositeData) attribute
                    .getValue();

                cluster.setClientConnectionCount(getLongAttribute(nscConnStats
                    .get(PulseConstants.COMPOSITE_DATA_KEY_CONNECTIONSACTIVE),
                    PulseConstants.COMPOSITE_DATA_KEY_CONNECTIONSACTIVE));
              }
            }
            break;
          }

        } catch (MalformedObjectNameException e) {
          LOGGER.warning(e);
        } catch (NullPointerException e) {
          LOGGER.warning(e);
        }

      }

    } catch (InstanceNotFoundException infe) {
      LOGGER.warning(infe);
    } catch (ReflectionException re) {
      LOGGER.warning(re);
    } catch (MBeanException anfe) {
      LOGGER.warning(anfe);
    }
  }

  /**
   * function used to get attribute values of Gateway Receiver and map them to
   * GatewayReceiver inner class object
   *
   * @param mbeanName
   * @return GatewayReceiver object
   * @throws InstanceNotFoundException
   * @throws IntrospectionException
   * @throws ReflectionException
   * @throws IOException
   * @throws AttributeNotFoundException
   * @throws MBeanException
   *
   *
   */
  private Cluster.GatewayReceiver initGatewayReceiver(ObjectName mbeanName)
      throws InstanceNotFoundException, IntrospectionException,
      ReflectionException, IOException, AttributeNotFoundException,
      MBeanException {

    Cluster.GatewayReceiver gatewayReceiver = new Cluster.GatewayReceiver();

    AttributeList attributeList = this.mbs.getAttributes(mbeanName,
        PulseConstants.GATEWAY_MBEAN_ATTRIBUTES);

    for (int i = 0; i < attributeList.size(); i++) {
      Attribute attribute = (Attribute) attributeList.get(i);

      if (attribute.getName().equals(PulseConstants.MBEAN_ATTRIBUTE_PORT)) {
        gatewayReceiver.setListeningPort(getIntegerAttribute(
            attribute.getValue(), attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_EVENTRECEIVEDDATE)) {
        gatewayReceiver.setLinkThroughput(getFloatAttribute(
            attribute.getValue(), attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_AVEARGEBATCHPROCESSINGTIME)) {
        gatewayReceiver.setAvgBatchProcessingTime(getLongAttribute(
            attribute.getValue(), attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_RUNNING)) {
        gatewayReceiver.setStatus(getBooleanAttribute(attribute.getValue(),
            attribute.getName()));
      }
    }
    return gatewayReceiver;
  }

  /**
   * function used to get attribute values of Gateway Sender and map them to
   * GatewaySender inner class object
   *
   * @param mbeanName
   * @return
   * @throws InstanceNotFoundException
   * @throws IntrospectionException
   * @throws ReflectionException
   * @throws IOException
   * @throws AttributeNotFoundException
   * @throws MBeanException
   */
  private Cluster.GatewaySender initGatewaySender(ObjectName mbeanName)
      throws InstanceNotFoundException, IntrospectionException,
      ReflectionException, IOException, AttributeNotFoundException,
      MBeanException {

    Cluster.GatewaySender gatewaySender = new Cluster.GatewaySender();
    AttributeList attributeList = this.mbs.getAttributes(mbeanName,
        PulseConstants.GATEWAYSENDER_MBEAN_ATTRIBUTES);

    for (int i = 0; i < attributeList.size(); i++) {
      Attribute attribute = (Attribute) attributeList.get(i);
      if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_EVENTRECEIVEDDATE)) {
        gatewaySender.setLinkThroughput(getFloatAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_BATCHSIZE)) {
        gatewaySender.setBatchSize(getIntegerAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_SENDERID)) {
        gatewaySender.setId(getStringAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_EVENTQUEUESIZE)) {
        gatewaySender.setQueueSize(getIntegerAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_RUNNING)) {
        gatewaySender.setStatus(getBooleanAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_PRIMARY)) {
        gatewaySender.setPrimary(getBooleanAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_PERSISTENCEENABLED)) {
        gatewaySender.setPersistenceEnabled(getBooleanAttribute(
            attribute.getValue(), attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_PARALLEL)) {
        gatewaySender.setSenderType(getBooleanAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_REMOTE_DS_ID)) {
        gatewaySender.setRemoteDSId(getIntegerAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_EVENTS_EXCEEDING_ALERT_THRESHOLD)) {
        gatewaySender.setEventsExceedingAlertThreshold(getIntegerAttribute(attribute.getValue(),
            attribute.getName()));
      }
    }
    return gatewaySender;
  }

  /**
   * function used for getting list of Gateway Senders from mBean for giving
   * member and update the list of gateway senders for respective member object
   */
  private void updateGatewaySender(ObjectName mbeanName) throws IOException {

    try {
      String memberName = mbeanName
          .getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

      if (cluster.getMembersHMap().containsKey(memberName)) {
        Cluster.Member existingMember = cluster.getMembersHMap()
            .get(memberName);
        Cluster.GatewaySender gatewaySender = initGatewaySender(mbeanName);
        for (Iterator<Cluster.GatewaySender> it = existingMember
            .getGatewaySenderList().iterator(); it.hasNext();) {
          Cluster.GatewaySender exisGatewaySender = it.next();
          if ((exisGatewaySender.getId()).equals(gatewaySender.getId())) {
            it.remove();
            break;
          }
        }

        // Add gateway sender
        existingMember.getGatewaySenderList().add(gatewaySender);

      } else {
        Cluster.Member member = new Cluster.Member();
        member.setName(memberName);
        member.setId(memberName);
        Cluster.GatewaySender gatewaySender = initGatewaySender(mbeanName);
        member.getGatewaySenderList().add(gatewaySender);
        cluster.getMembersHMap().put(memberName, member);
      }
    } catch (InstanceNotFoundException infe) {
      LOGGER.warning(infe);
    } catch (ReflectionException re) {
      LOGGER.warning(re);
    } catch (MBeanException me) {
      LOGGER.warning(me);
    } catch (AttributeNotFoundException anfe) {
      LOGGER.warning(anfe);
    } catch (IntrospectionException ire) {
      LOGGER.warning(ire);
    }
  }

  /**
   * function used to get attribute values of Async Event Queue and map them to
   * Async Event Queue  inner class object
   *
   * @param mbeanName
   * @return
   * @throws InstanceNotFoundException
   * @throws IntrospectionException
   * @throws ReflectionException
   * @throws IOException
   * @throws AttributeNotFoundException
   * @throws MBeanException
   */
  private Cluster.AsyncEventQueue initAsyncEventQueue(ObjectName mbeanName)
      throws InstanceNotFoundException, IntrospectionException,
      ReflectionException, IOException, AttributeNotFoundException,
      MBeanException {

    Cluster.AsyncEventQueue asyncEventQueue = new Cluster.AsyncEventQueue();
    AttributeList attributeList = this.mbs.getAttributes(mbeanName,
        PulseConstants.ASYNC_EVENT_QUEUE_MBEAN_ATTRIBUTES);

    for (int i = 0; i < attributeList.size(); i++) {
      Attribute attribute = (Attribute) attributeList.get(i);
      if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_AEQ_ASYNCEVENTID)) {
        asyncEventQueue.setId(getStringAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_AEQ_ASYNC_EVENT_LISTENER)) {
        asyncEventQueue.setAsyncEventListener(getStringAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_AEQ_BATCH_CONFLATION_ENABLED)) {
        asyncEventQueue.setBatchConflationEnabled(getBooleanAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_AEQ_BATCH_TIME_INTERVAL)) {
        asyncEventQueue.setBatchTimeInterval(getLongAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_AEQ_BATCH_SIZE)) {
        asyncEventQueue.setBatchSize(getIntegerAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_AEQ_EVENT_QUEUE_SIZE)) {
        asyncEventQueue.setEventQueueSize(getIntegerAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_AEQ_PARALLEL)) {
        asyncEventQueue.setParallel(getBooleanAttribute(
            attribute.getValue(), attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_AEQ_PRIMARY)) {
        asyncEventQueue.setPrimary(getBooleanAttribute(attribute.getValue(),
            attribute.getName()));
      }
    }
    return asyncEventQueue;
  }

  /**
   * function used for getting list of Gateway Senders from mBean for giving
   * member and update the list of gateway senders for respective member object
   */
  private void updateAsyncEventQueue(ObjectName mbeanName) throws IOException {
    try {
      String memberName = mbeanName
          .getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

      if (cluster.getMembersHMap().containsKey(memberName)) {
        Cluster.Member existingMember = cluster.getMembersHMap()
            .get(memberName);
        Cluster.AsyncEventQueue asyncQ = initAsyncEventQueue(mbeanName);
        for (Iterator<Cluster.AsyncEventQueue> it = existingMember.getAsyncEventQueueList().iterator(); it.hasNext();) {
          Cluster.AsyncEventQueue exisAsyncEventQueue = it.next();
          if ((exisAsyncEventQueue.getId()).equals(asyncQ.getId())) {
            it.remove();
            break;
          }
        }

        // Add async event queue
        existingMember.getAsyncEventQueueList().add(asyncQ);
      } else {
        Cluster.Member member = new Cluster.Member();
        member.setName(memberName);
        member.setId(memberName);

        Cluster.AsyncEventQueue asyncQ = initAsyncEventQueue(mbeanName);
        member.getAsyncEventQueueList().add(asyncQ);

        cluster.getMembersHMap().put(memberName, member);
      }
    } catch (InstanceNotFoundException infe) {
      LOGGER.warning(infe);
    } catch (ReflectionException re) {
      LOGGER.warning(re);
    } catch (MBeanException me) {
      LOGGER.warning(me);
    } catch (AttributeNotFoundException anfe) {
      LOGGER.warning(anfe);
    } catch (IntrospectionException ire) {
      LOGGER.warning(ire);
    }
  }

  /**
   * function used for getting a Gateway Receiver from mBean for giving member
   * and update the gateway receiver for respective member object
   */
  private void updateGatewayReceiver(ObjectName mbeanName) throws IOException {

    try {
      String memberName = mbeanName
          .getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

      if (cluster.getMembersHMap().containsKey(memberName)) {
        Cluster.Member existingMember = cluster.getMembersHMap()
            .get(memberName);
        Cluster.GatewayReceiver gatewayReceiver = initGatewayReceiver(mbeanName);
        existingMember.setGatewayReceiver(gatewayReceiver);
      } else {
        Cluster.Member member = new Cluster.Member();
        member.setName(memberName);
        member.setId(memberName);
        Cluster.GatewayReceiver gatewayReceiver = initGatewayReceiver(mbeanName);
        member.setGatewayReceiver(gatewayReceiver);
        cluster.getMembersHMap().put(memberName, member);
      }
    } catch (InstanceNotFoundException infe) {
      LOGGER.warning(infe);
    } catch (ReflectionException re) {
      LOGGER.warning(re);
    } catch (MBeanException me) {
      LOGGER.warning(me);
    } catch (AttributeNotFoundException anfe) {
      LOGGER.warning(anfe);
    } catch (IntrospectionException ire) {
      LOGGER.warning(ire);
    }
  }

  /**
   * function used for getting member clients from mbean and update the clients
   * information in member object's client arraylist
   */
  private void updateMemberClient(ObjectName mbeanName) throws IOException {

    try {
      String memberName = mbeanName
          .getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

      if (cluster.getMembersHMap().containsKey(memberName)) {
        Cluster.Member existingMember = cluster.getMembersHMap()
            .get(memberName);
        HashMap<String, Cluster.Client> memberClientsHM = new HashMap<String, Cluster.Client>();

        existingMember.setMemberPort(""
            + this.mbs.getAttribute(mbeanName,
                PulseConstants.MBEAN_ATTRIBUTE_PORT));

        CompositeData[] compositeData = (CompositeData[]) (this.mbs.invoke(
            mbeanName, PulseConstants.MBEAN_OPERATION_SHOWALLCLIENTS, null,
            null));
        for (CompositeData cmd : compositeData) {
          Cluster.Client client = new Cluster.Client();
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_CLIENTID)) {
            client.setId((String) cmd
                .get(PulseConstants.COMPOSITE_DATA_KEY_CLIENTID));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_NAME)) {
            client.setName((String) cmd
                .get(PulseConstants.COMPOSITE_DATA_KEY_NAME));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_HOSTNAME)) {
            client.setHost((String) cmd
                .get(PulseConstants.COMPOSITE_DATA_KEY_HOSTNAME));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_QUEUESIZE)) {
            client.setQueueSize((Integer) cmd
                .get(PulseConstants.COMPOSITE_DATA_KEY_QUEUESIZE));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_PROCESSCPUTIME)) {
            client.setProcessCpuTime((Long) cmd
                .get(PulseConstants.COMPOSITE_DATA_KEY_PROCESSCPUTIME));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_UPTIME)) {
            client.setUptime((Long) cmd
                .get(PulseConstants.COMPOSITE_DATA_KEY_UPTIME));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_NUMOFTHREADS)) {
            client.setThreads((Integer) cmd
                .get(PulseConstants.COMPOSITE_DATA_KEY_NUMOFTHREADS));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_NUMOFGETS)) {
            client.setGets((Integer) cmd
                .get(PulseConstants.COMPOSITE_DATA_KEY_NUMOFGETS));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_NUMOFPUTS)) {
            client.setPuts((Integer) cmd
                .get(PulseConstants.COMPOSITE_DATA_KEY_NUMOFPUTS));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_CPUS)) {
            client.setCpus((Integer) cmd
                .get(PulseConstants.COMPOSITE_DATA_KEY_CPUS));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_CPUS)) {
            client.setCpuUsage(0);
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_CONNECTED)){
            client.setConnected((Boolean) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_CONNECTED));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_CLIENTCQCOUNT)){ 
        	client.setClientCQCount((Integer) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_CLIENTCQCOUNT)); 
          } 
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_SUBSCRIPTIONENABLED)){ 
            client.setSubscriptionEnabled((Boolean) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_SUBSCRIPTIONENABLED)); 
          } 
          memberClientsHM.put(client.getId(), client);
        }
        existingMember.updateMemberClientsHMap(memberClientsHM);
      }
    } catch (InstanceNotFoundException infe) {
      LOGGER.warning(infe);
    } catch (ReflectionException re) {
      LOGGER.warning(re);
    } catch (MBeanException me) {
      LOGGER.warning(me);
    } catch (AttributeNotFoundException anfe) {
      LOGGER.warning(anfe);
    }
  }

  /**
   * Add member specific region information on the region
   *
   * @param regionFullPath
   * @param region
   */
  private void updateRegionOnMembers(String regionFullPath, Cluster.Region region) throws IOException {

    try{
        List<String> memberNamesTemp = region.getMemberName();
        ArrayList<String> memberNames = new ArrayList<String>(memberNamesTemp);

        List<Cluster.RegionOnMember> regionOnMemberList = new ArrayList<Cluster.RegionOnMember>();
        List<Cluster.RegionOnMember> regionOnMemberListNew = new ArrayList<Cluster.RegionOnMember>();
        Cluster.RegionOnMember[] regionOnMemberNames = region.getRegionOnMembers();

        if ((regionOnMemberNames != null) && (regionOnMemberNames.length > 0)) {
          regionOnMemberList = new ArrayList<Cluster.RegionOnMember>(Arrays.asList(regionOnMemberNames));
        }
        LOGGER.fine("updateRegionOnMembers : # regionOnMembers objects in region = " + regionOnMemberList.size());

        for(Cluster.RegionOnMember anRom : regionOnMemberList) {

          for(String memberName : memberNames){
            if(anRom.getMemberName().equals(memberName)){
              // Add regionOnMember object in new list
              regionOnMemberListNew.add(anRom);

              LOGGER.fine("updateRegionOnMembers : Processing existing Member name = " + anRom.getMemberName());
              String objectNameROM = PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_REGION + regionFullPath + PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_MEMBER + anRom.getMemberName();
              ObjectName regionOnMemberMBean = new ObjectName(objectNameROM);
              LOGGER.fine("updateRegionOnMembers : Object name = " + regionOnMemberMBean.getCanonicalName());

              AttributeList attributeList = this.mbs.getAttributes(regionOnMemberMBean, PulseConstants.REGION_ON_MEMBER_MBEAN_ATTRIBUTES);
              for (int i = 0; i < attributeList.size(); i++) {
                Attribute attribute = (Attribute) attributeList.get(i);
                  if (attribute.getName().equals(
                      PulseConstants.MBEAN_ATTRIBUTE_ENTRYSIZE)) {
                    anRom.setEntrySize(getLongAttribute(attribute.getValue(),
                        attribute.getName()));
                    LOGGER.fine("updateRegionOnMembers : anRom.getEntrySize() = " + anRom.getEntrySize());
                  } else if (attribute.getName().equals(
                      PulseConstants.MBEAN_ATTRIBUTE_ENTRYCOUNT)) {
                    anRom.setEntryCount(getLongAttribute(attribute.getValue(),
                        attribute.getName()));
                    LOGGER.fine("updateRegionOnMembers : anRom.getEntryCount() = " + anRom.getEntryCount());
                  } else if (attribute.getName().equals(
                      PulseConstants.MBEAN_ATTRIBUTE_PUTSRATE)) {
                    anRom.setPutsRate(getFloatAttribute(attribute.getValue(),
                        attribute.getName()));
                    LOGGER.fine("updateRegionOnMembers : anRom.getPutsRate() = " + anRom.getPutsRate());
                  } else if (attribute.getName().equals(
                      PulseConstants.MBEAN_ATTRIBUTE_GETSRATE)) {
                    anRom.setGetsRate(getFloatAttribute(attribute.getValue(),
                        attribute.getName()));
                    LOGGER.fine("updateRegionOnMembers : anRom.getGetsRate() = " + anRom.getGetsRate());
                  } else if (attribute.getName().equals(
                      PulseConstants.MBEAN_ATTRIBUTE_DISKREADSRATE)) {
                    anRom.setDiskGetsRate(getFloatAttribute(attribute.getValue(),
                        attribute.getName()));
                    LOGGER.fine("updateRegionOnMembers : anRom.getDiskGetsRate() = " + anRom.getDiskGetsRate());
                  } else if (attribute.getName().equals(
                      PulseConstants.MBEAN_ATTRIBUTE_DISKWRITESRATE)) {
                    anRom.setDiskPutsRate(getFloatAttribute(attribute.getValue(),
                        attribute.getName()));
                    LOGGER.fine("updateRegionOnMembers : anRom.getDiskPutsRate() = " + anRom.getDiskPutsRate());
                  } else if (attribute.getName().equals(
                      PulseConstants.MBEAN_ATTRIBUTE_LOCALMAXMEMORY)) {
                    anRom.setLocalMaxMemory(getIntegerAttribute(attribute.getValue(),
                        attribute.getName()));
                    LOGGER.fine("updateRegionOnMembers : anRom.getLocalMaxMemory() = " + anRom.getLocalMaxMemory());
                  }
              }

              anRom.getGetsPerSecTrend().add(anRom.getGetsRate());
              anRom.getPutsPerSecTrend().add(anRom.getPutsRate());
              anRom.getDiskReadsPerSecTrend().add(anRom.getDiskGetsRate());
              anRom.getDiskWritesPerSecTrend().add(anRom.getDiskPutsRate());
              LOGGER.fine("updateRegionOnMembers : Existing member on region : getGetsRate() = " + anRom.getGetsPerSecTrend().size() + ", getPutsRate() = "
                  + anRom.getPutsPerSecTrend().size() + ", getDiskGetsRate() = " + anRom.getDiskReadsPerSecTrend().size() + ", getDiskPutsRate() = "
                  + anRom.getDiskWritesPerSecTrend().size());

              //remove existing member names from list so only new ones will remain
              memberNames.remove(anRom.getMemberName());

              break;
            }
          }
        }

        LOGGER.fine("updateRegionOnMembers : Loop over remaining member names and adding new member in region. Existing count = " + regionOnMemberList.size());
        LOGGER.fine("updateRegionOnMembers : Remaining new members in this region = " + memberNames.size());
        //loop over the remaining regions members and add new members for this region
        for(String memberName : memberNames) {
          String objectNameROM = PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_REGION + regionFullPath + PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_MEMBER + memberName;
          ObjectName regionOnMemberMBean = new ObjectName(objectNameROM);
          Cluster.RegionOnMember regionOnMember = new Cluster.RegionOnMember();
          regionOnMember.setMemberName(memberName);
          regionOnMember.setRegionFullPath(regionFullPath);
          AttributeList attributeList = this.mbs.getAttributes(regionOnMemberMBean, PulseConstants.REGION_ON_MEMBER_MBEAN_ATTRIBUTES);
          for (int i = 0; i < attributeList.size(); i++) {
            Attribute attribute = (Attribute) attributeList.get(i);
              if (attribute.getName().equals(
                  PulseConstants.MBEAN_ATTRIBUTE_ENTRYSIZE)) {
                regionOnMember.setEntrySize(getLongAttribute(attribute.getValue(),
                    attribute.getName()));
              } else if (attribute.getName().equals(
                  PulseConstants.MBEAN_ATTRIBUTE_ENTRYCOUNT)) {
                regionOnMember.setEntryCount(getLongAttribute(attribute.getValue(),
                    attribute.getName()));
              } else if (attribute.getName().equals(
                  PulseConstants.MBEAN_ATTRIBUTE_PUTSRATE)) {
                regionOnMember.setPutsRate(getFloatAttribute(attribute.getValue(),
                    attribute.getName()));
              } else if (attribute.getName().equals(
                  PulseConstants.MBEAN_ATTRIBUTE_GETSRATE)) {
                regionOnMember.setGetsRate(getFloatAttribute(attribute.getValue(),
                    attribute.getName()));
              } else if (attribute.getName().equals(
                  PulseConstants.MBEAN_ATTRIBUTE_DISKREADSRATE)) {
                regionOnMember.setDiskGetsRate(getFloatAttribute(attribute.getValue(),
                    attribute.getName()));
              } else if (attribute.getName().equals(
                  PulseConstants.MBEAN_ATTRIBUTE_DISKWRITESRATE)) {
                regionOnMember.setDiskPutsRate(getFloatAttribute(attribute.getValue(),
                    attribute.getName()));
              } else if (attribute.getName().equals(
                  PulseConstants.MBEAN_ATTRIBUTE_LOCALMAXMEMORY)) {
                regionOnMember.setLocalMaxMemory(getIntegerAttribute(attribute.getValue(),
                    attribute.getName()));
              }
          }

          regionOnMember.getGetsPerSecTrend().add(regionOnMember.getGetsRate());
          regionOnMember.getPutsPerSecTrend().add(regionOnMember.getPutsRate());
          regionOnMember.getDiskReadsPerSecTrend().add(regionOnMember.getDiskGetsRate());
          regionOnMember.getDiskWritesPerSecTrend().add(regionOnMember.getDiskPutsRate());

          LOGGER.fine("updateRegionOnMembers : Adding New member on region : getGetsRate() = " + regionOnMember.getGetsRate() + ", getPutsRate() = "
              + regionOnMember.getPutsRate() + ", getDiskGetsRate() = " + regionOnMember.getDiskGetsRate() + ", getDiskPutsRate() = "
              + regionOnMember.getDiskPutsRate());

          regionOnMemberListNew.add(regionOnMember);
        }

        //set region on member
        region.setRegionOnMembers(regionOnMemberListNew);
        LOGGER.fine("updateRegionOnMembers : Total regions on member in region " + region.getFullPath() + " after update = " + region.getRegionOnMembers().length);
    } catch (MalformedObjectNameException e) {
      LOGGER.warning(e);
    } catch (InstanceNotFoundException infe) {
      LOGGER.warning(infe);
    } catch (ReflectionException re) {
      LOGGER.warning(re);
    }
  }

  /**
   * function used to get attribute values of Cluster Region and map them to
   * cluster region vo
   *
   * @param mbeanName
   *          Cluster Region MBean
   */
  private void updateClusterRegion(ObjectName mbeanName) throws IOException {

    try {

      AttributeList attributeList = this.mbs.getAttributes(mbeanName,
          PulseConstants.REGION_MBEAN_ATTRIBUTES);

      // retrieve the full path of the region
      String regionFullPath = null;
      for (int i = 0; i < attributeList.size(); i++) {
        Attribute attribute = (Attribute) attributeList.get(i);

        if (attribute.getName().equals(PulseConstants.MBEAN_ATTRIBUTE_FULLPATH)) {
          regionFullPath = getStringAttribute(attribute.getValue(),
              attribute.getName());
          break;
        }
      }

      Cluster.Region region = cluster.getClusterRegions().get(regionFullPath);

      if (null == region) {
        region = new Cluster.Region();
      }

      for (int i = 0; i < attributeList.size(); i++) {

        Attribute attribute = (Attribute) attributeList.get(i);

        if (attribute.getName().equals(PulseConstants.MBEAN_ATTRIBUTE_MEMBERS)) {
          String memName[] = (String[]) attribute.getValue();
          region.getMemberName().clear();
          for (int k = 0; k < memName.length; k++) {
            region.getMemberName().add(memName[k]);
          }
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_FULLPATH)) {
          region.setFullPath(getStringAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_DISKREADSRATE)) {
          region.setDiskReadsRate(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_DISKWRITESRATE)) {
          region.setDiskWritesRate(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_EMPTYNODES)) {
          region.setEmptyNode(getIntegerAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_GETSRATE)) {
          region.setGetsRate(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_LRUEVICTIONRATE)) {
          region.setLruEvictionRate(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_PUTSRATE)) {
          region.setPutsRate(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_REGIONTYPE)) {
          region.setRegionType(getStringAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_ENTRYSIZE)) {
          region.setEntrySize(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_SYSTEMREGIONENTRYCOUNT)) {
          region.setSystemRegionEntryCount(getLongAttribute(
              attribute.getValue(), attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_MEMBERCOUNT)) {
          region.setMemberCount(getIntegerAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_PERSISTENTENABLED)) {
          region.setPersistentEnabled(getBooleanAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_NAME)) {
          region.setName(getStringAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_GATEWAYENABLED)) {
          region.setWanEnabled(getBooleanAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_DISKUSAGE)) {
          region.setDiskUsage(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        }
      }

      /* GemfireXD related code
      try{// Added for Rolling upgrade
    	  CompositeData compositeData = (CompositeData) (this.mbs.invoke(mbeanName,
    	          PulseConstants.MBEAN_OPERATION_LISTREGIONATTRIBUTES, null, null));

    	      if (compositeData != null) {
    	        if (compositeData
    	            .containsKey(PulseConstants.COMPOSITE_DATA_KEY_COMPRESSIONCODEC)) {
    	          String regCompCodec = (String) compositeData
    	              .get(PulseConstants.COMPOSITE_DATA_KEY_COMPRESSIONCODEC);
    	          if (null != regCompCodec) {
    	            region.setCompressionCodec(regCompCodec);
    	          }
    	        }
    	        if (compositeData
    	            .containsKey(PulseConstants.COMPOSITE_DATA_KEY_ENABLEOFFHEAPMEMORY)) {
    	          region.setEnableOffHeapMemory((Boolean) compositeData
    	              .get(PulseConstants.COMPOSITE_DATA_KEY_ENABLEOFFHEAPMEMORY));
    	        }
    	        if (compositeData
    	            .containsKey(PulseConstants.COMPOSITE_DATA_KEY_HDFSWRITEONLY)) {
    	          region.setHdfsWriteOnly((Boolean) compositeData
    	              .get(PulseConstants.COMPOSITE_DATA_KEY_HDFSWRITEONLY));
    	        }
    	      }
      } catch (MBeanException anfe) {
          LOGGER.warning(anfe);
          region.setHdfsWriteOnly(false);
          region.setEnableOffHeapMemory(false);
          region.setCompressionCodec("NA");
      }catch (javax.management.RuntimeMBeanException invalidOe) {
    	    region.setHdfsWriteOnly(false);
            region.setEnableOffHeapMemory(false);
            region.setCompressionCodec("NA");
           // LOGGER.info("Some of the Pulse elements are not available currently. There might be a GemFire upgrade going on.");
      }
      */

      // TODO : Uncomment below code when sql fire mbean attributes are
      // available
      /*
       * // IF SQLFIRE if
       * (PulseConstants.PRODUCT_NAME_SQLFIRE.equalsIgnoreCase(PulseController
       * .getPulseProductSupport())) {
       *
       * try { String tableName = this.getTableNameFromRegionName(region
       * .getFullPath());
       *
       * ObjectName tableObjName = new ObjectName(
       * PulseConstants.OBJECT_NAME_TABLE_AGGREGATE_PATTERN + tableName);
       *
       * AttributeList tableAttributeList = this.mbs.getAttributes(
       * tableObjName, PulseConstants.SF_TABLE_MBEAN_ATTRIBUTES);
       *
       * for (int i = 0; i < tableAttributeList.size(); i++) {
       *
       * Attribute attribute = (Attribute) tableAttributeList.get(i);
       *
       * if (attribute.getName().equals(
       * PulseConstants.MBEAN_ATTRIBUTE_ENTRYSIZE)) {
       * System.out.println("[SQLfire] setting entry size");
       * region.setEntrySize(getLongAttribute(attribute.getValue(),
       * attribute.getName())); } else if (attribute.getName().equals(
       * PulseConstants.MBEAN_ATTRIBUTE_NUMBEROFROWS)) {
       * System.out.println("[SQLfire] setting num of rows");
       * region.setSystemRegionEntryCount(getLongAttribute(
       * attribute.getValue(), attribute.getName())); } } } catch
       * (MalformedObjectNameException e) { LOGGER.warning(e); } catch
       * (NullPointerException e) { LOGGER.warning(e); } }
       */

      // Add to map even if region is present. If region is already there it
      // will be a no-op.

      //add for each member
      updateRegionOnMembers(regionFullPath, region);

      cluster.addClusterRegion(regionFullPath, region);
      cluster.getDeletedRegions().remove(region.getFullPath());
      // Memory Reads and writes
      region.getPutsPerSecTrend().add(region.getPutsRate());
      region.getGetsPerSecTrend().add(region.getGetsRate());
      // Disk Reads and Writes
      region.getDiskReadsPerSecTrend().add(region.getDiskReadsRate());
      region.getDiskWritesPerSecTrend().add(region.getDiskWritesRate());

    } catch (InstanceNotFoundException infe) {
      LOGGER.warning(infe);
    } catch (ReflectionException re) {
      LOGGER.warning(re);
    } 
  }

  private static boolean isQuoted(String value) {
    final int len = value.length();
    if (len < 2 || value.charAt(0) != '"' || value.charAt(len - 1) != '"') {
      return false;
    } else {
      return true;
    }
  }

  private void updateClusterStatement(ObjectName mbeanName) throws IOException {

    try {

      AttributeList attributeList = this.mbs.getAttributes(mbeanName,
          PulseConstants.STATEMENT_MBEAN_ATTRIBUTES);
      // retrieve the full path of the region
      String statementDefinition = mbeanName.getKeyProperty("name");

      if (isQuoted(statementDefinition)) {
        statementDefinition = ObjectName.unquote(statementDefinition);
      }

      Cluster.Statement statement = cluster.getClusterStatements().get(
          statementDefinition);

      if (null == statement) {
        statement = new Cluster.Statement();
        statement.setQueryDefinition(statementDefinition);
      }

      for (int i = 0; i < attributeList.size(); i++) {

        Attribute attribute = (Attribute) attributeList.get(i);

        if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_NUMTIMESCOMPILED)) {
          statement.setNumTimesCompiled(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_NUMEXECUTION)) {
          statement.setNumExecution(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_NUMEXECUTIONSINPROGRESS)) {
          statement.setNumExecutionsInProgress(getLongAttribute(
              attribute.getValue(), attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_NUMTIMESGLOBALINDEXLOOKUP)) {
          statement.setNumTimesGlobalIndexLookup(getLongAttribute(
              attribute.getValue(), attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_NUMROWSMODIFIED)) {
          statement.setNumRowsModified(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_PARSETIME)) {
          statement.setParseTime(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_BINDTIME)) {
          statement.setBindTime(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_OPTIMIZETIME)) {
          statement.setOptimizeTime(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_ROUTINGINFOTIME)) {
          statement.setRoutingInfoTime(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_GENERATETIME)) {
          statement.setGenerateTime(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_TOTALCOMPILATIONTIME)) {
          statement.setTotalCompilationTime(getLongAttribute(
              attribute.getValue(), attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_EXECUTIONTIME)) {
          statement.setExecutionTime(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_PROJECTIONTIME)) {
          statement.setProjectionTime(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_TOTALEXECUTIONTIME)) {
          statement.setTotalExecutionTime(getLongAttribute(
              attribute.getValue(), attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_ROWSMODIFICATIONTIME)) {
          statement.setRowsModificationTime(getLongAttribute(
              attribute.getValue(), attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_QNNUMROWSSEEN)) {
          statement.setqNNumRowsSeen(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_QNMSGSENDTIME)) {
          statement.setqNMsgSendTime(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_QNMSGSERTIME)) {
          statement.setqNMsgSerTime(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        }
        if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_QNRESPDESERTIME)) {
          statement.setqNRespDeSerTime(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        }
      }

      cluster.addClusterStatement(statementDefinition, statement);
      // TODO : to store data for sparklines later
      /*
       * region.getPutsPerSecTrend().add(region.getPutsRate());
       * region.getGetsPerSecTrend().add(region.getGetsRate());
       */
    } catch (InstanceNotFoundException infe) {
      LOGGER.warning(infe);
    } catch (ReflectionException re) {
      LOGGER.warning(re);
    }
  }

  /**
   * function used to iterate through all member attributes and return the
   * updated member
   */
  private Cluster.Member initializeMember(ObjectName mbeanName,
      Cluster.Member member) throws InstanceNotFoundException,
      ReflectionException, IOException {

    AttributeList attributeList = this.mbs.getAttributes(mbeanName,
        PulseConstants.MEMBER_MBEAN_ATTRIBUTES);

    for (int i = 0; i < attributeList.size(); i++) {

      Attribute attribute = (Attribute) attributeList.get(i);

      if (attribute.getName().equals(PulseConstants.MBEAN_ATTRIBUTE_GEMFIREVERSION)) {
        if (member.getGemfireVersion() == null) {
          // Set Member's GemFire Version if not set already
          String gemfireVersion = obtainGemfireVersion(getStringAttribute(
              attribute.getValue(), attribute.getName()));
          member.setGemfireVersion(gemfireVersion);
        }
      } else if (attribute.getName().equals(PulseConstants.MBEAN_ATTRIBUTE_MANAGER)) {
        member.setManager(getBooleanAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_TOTALREGIONCOUNT)) {
        member.setTotalRegionCount(getIntegerAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_LOCATOR)) {
        member.setLocator(getBooleanAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_TOTALDISKUSAGE)) {
        member.setTotalDiskUsage(getLongAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_SERVER)) {
        member.setServer(getBooleanAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_TOTALFILEDESCRIPTOROPEN)) {
        member.setTotalFileDescriptorOpen(getLongAttribute(
            attribute.getValue(), attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_LOADAVERAGE)) {
        member.setLoadAverage(getDoubleAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_DISKWRITESRATE)) {
        member.setThroughputWrites(getFloatAttribute(attribute.getValue(),
            attribute.getName()));
        member.getThroughputWritesTrend().add(member.getThroughputWrites());
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_DISKREADSRATE)) {
        member.setThroughputReads(getFloatAttribute(attribute.getValue(),
            attribute.getName()));
        member.getThroughputReadsTrend().add(member.getThroughputReads());
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_JVMPAUSES)) {
        long trendVal = determineCurrentJVMPauses(
            PulseConstants.JVM_PAUSES_TYPE_MEMBER, member.getName(),
            getLongAttribute(attribute.getValue(),
            attribute.getName()));
        member.setGarbageCollectionCount(trendVal);
        member.getGarbageCollectionSamples().add(
            member.getGarbageCollectionCount());
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_USEDMEMORY)) {
        member.setCurrentHeapSize(getLongAttribute(attribute.getValue(),
            attribute.getName()));
        member.getHeapUsageSamples().add(member.getCurrentHeapSize());
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_MAXMEMORY)) {
        member.setMaxHeapSize(getLongAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_NUMTHREADS)) {
        member.setNumThreads(getIntegerAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_MEMBERUPTIME)) {
        member.setUptime(getLongAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName()
          .equals(PulseConstants.MBEAN_ATTRIBUTE_HOST)) {
        member.setHost(getStringAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_TOTALBYTESONDISK)) {
        member.setTotalBytesOnDisk(getLongAttribute(attribute.getValue(),
            attribute.getName()));
        member.getTotalBytesOnDiskSamples().add(member.getTotalBytesOnDisk());
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_CPUUSAGE)) {
        member.setCpuUsage(getFloatAttribute(attribute.getValue(),
            attribute.getName()));
        member.getCpuUsageSamples().add(member.getCpuUsage());
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_HOSTCPUUSAGE)) {
        // Float value is expected for host cpu usage.
        // TODO Remove Float.valueOf() when float value is provided in mbean
        member.setHostCpuUsage(Float.valueOf(getIntegerAttribute(
            attribute.getValue(), attribute.getName())));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_MEMBER)) {
        member.setName(getStringAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(PulseConstants.MBEAN_ATTRIBUTE_ID)) {
        member.setId(getStringAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_AVERAGEREADS)) {
        member.setGetsRate(getFloatAttribute(attribute.getValue(),
            attribute.getName()));
        member.getGetsPerSecond().add(member.getGetsRate());
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_AVERAGEWRITES)) {
        member.setPutsRate(getFloatAttribute(attribute.getValue(),
            attribute.getName()));
        member.getPutsPerSecond().add(member.getPutsRate());
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_OFFHEAPFREESIZE)) {
        member.setOffHeapFreeSize(getLongAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_OFFHEAPUSEDSIZE)) {
        member.setOffHeapUsedSize(getLongAttribute(attribute.getValue(),
            attribute.getName()));
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_SERVERGROUPS)) {
        String sgValues[] = (String[]) attribute.getValue();
        member.getServerGroups().clear();
        for (int k = 0; k < sgValues.length; k++) {
          member.getServerGroups().add(sgValues[k]);
        }
      } else if (attribute.getName().equals(
          PulseConstants.MBEAN_ATTRIBUTE_REDUNDANCYZONES)) {
        /*String rzValues[] = (String[]) attribute.getValue();
        member.getRedundancyZones().clear();
        for (int k = 0; k < rzValues.length; k++) {
          member.getRedundancyZones().add(rzValues[k]);
        }*/

        String rzValue = "";
        if(null != attribute.getValue()){
          rzValue = getStringAttribute(attribute.getValue(),
              attribute.getName());
        }
        member.getRedundancyZones().clear();
        if(!rzValue.isEmpty()){
          member.getRedundancyZones().add(rzValue);
        }
      }
    }

    // SQLFire specific attributes
    if (PulseController.getPulseProductSupport().equalsIgnoreCase(
        PulseConstants.PRODUCT_NAME_SQLFIRE)) {

      try {
        // get sqlfire mbeans
        String memberName = mbeanName
            .getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

        ObjectName sfMemberMbeansObjectName = new ObjectName(
            PulseConstants.OBJECT_NAME_SF_MEMBER_PATTERN + memberName);

        Set<ObjectName> sfMemberMBeans = this.mbs.queryNames(
            sfMemberMbeansObjectName, null);
        for (ObjectName sfMemberMBean : sfMemberMBeans) {

          AttributeList attrList = this.mbs.getAttributes(sfMemberMBean,
              PulseConstants.SF_MEMBER_MBEAN_ATTRIBUTES);
          for (int i = 0; i < attrList.size(); i++) {

            Attribute attribute = (Attribute) attrList.get(i);

            if (attribute.getName().equals(
                PulseConstants.MBEAN_ATTRIBUTE_DATASTORE)) {
              member.setServer(getBooleanAttribute(attribute.getValue(),
                  attribute.getName()));

              // Update Server count
              if (member.isServer()) {
                cluster.setServerCount(cluster.getServerCount() + 1);
              }
            } else if (attribute.getName().equals(
                    PulseConstants.MBEAN_ATTRIBUTE_NETWORKSERVERCLIENTCONNECTIONSTATS)) {

              CompositeData nscConnStats = (CompositeData) attribute.getValue();

              // Update sqlfire client count
              member.setNumSqlfireClients(getLongAttribute(nscConnStats
                  .get(PulseConstants.COMPOSITE_DATA_KEY_CONNECTIONSACTIVE),
                  PulseConstants.COMPOSITE_DATA_KEY_CONNECTIONSACTIVE));
            }
          }
          break;
        }

      } catch (MalformedObjectNameException e) {
        LOGGER.warning(e);
      } catch (NullPointerException e) {
        LOGGER.warning(e);
      }

    }

    return member;
  }

  /**
   * function used to get attribute values of Cluster Member and map them to
   * cluster member vo
   *
   * @param mbeanName
   *          Cluster Member MBean
   */
  private void updateClusterMember(ObjectName mbeanName) throws IOException {

    try {
      String memberName = mbeanName
          .getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

      Cluster.Member clusterMember = cluster.getMembersHMap().get(memberName);

      if (clusterMember != null) // checking if member exists or not
      {
        cluster.getDeletedMembers().remove(memberName);
      } else {
        clusterMember = new Cluster.Member();
        cluster.getMembersHMap().put(memberName, clusterMember);
      }

      // initialize member and add to cluster's member list
      clusterMember = initializeMember(mbeanName, clusterMember);
      ArrayList<Cluster.Member> memberArrList = (ArrayList<Cluster.Member>) cluster
          .getPhysicalToMember().get(clusterMember.getHost());
      if (memberArrList != null) {
        if (!memberArrList.contains(clusterMember)) {
          memberArrList.add(clusterMember);
        }
      } else {
        ArrayList<Cluster.Member> memberList = new ArrayList<Cluster.Member>();
        memberList.add(clusterMember);
        cluster.getPhysicalToMember().put(clusterMember.getHost(), memberList);
      }
    } catch (InstanceNotFoundException infe) {
      LOGGER.warning(infe);
    } catch (ReflectionException re) {
      LOGGER.warning(re);
    }
  }

  // /**
  // * function used for creating member key with a combination
  // * of member id and and member name
  // * in key we are replacing ":" with "-" for both member id and name
  // * @param id
  // * @param name
  // * @return
  // */
  // private String getMemberNameOrId(String id, String name){
  // String key;
  // if (id != null) {
  // id = id.replace(":", "-");
  // }
  // if (name != null) {
  // name = name.replace(":", "-");
  // }
  // key = id+name;
  // return key;
  // }
  /**
   * function used to handle Float data type if the value for mbean for an
   * attribute is null then return 0.0 as default value else return the
   * attribute value
   */
  private Float getFloatAttribute(Object object, String name) {
    try {
      if (!(object.getClass().equals(Float.class))) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info("************************Unexpected type for attribute: "
              + name + " Expected type: " + Float.class.getName()
              + " Received type: " + object.getClass().getName()
              + "************************");
        }
        return Float.valueOf(0.0f);
      } else {
        return (Float) object;
      }
    } catch (Exception e) {
      if (LOGGER.infoEnabled()) {
        LOGGER.info("Exception Occured: " + e.getMessage());
      }
      return Float.valueOf(0.0f);
    }
  }

  /**
   * function used to handle Integer data type if the value for mbean for an
   * attribute is null then return 0 as default value else return the attribute
   * value
   */
  private Integer getIntegerAttribute(Object object, String name) {
    try {
      if (!(object.getClass().equals(Integer.class))) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info("************************Unexpected type for attribute: "
              + name + " Expected type: " + Integer.class.getName()
              + " Received type: " + object.getClass().getName()
              + "************************");
        }
        return Integer.valueOf(0);
      } else {
        return (Integer) object;
      }
    } catch (Exception e) {
      if (LOGGER.infoEnabled()) {
        LOGGER.info("Exception Occured: " + e.getMessage());
      }
      return Integer.valueOf(0);
    }
  }

  /**
   * function used to handle Long data type if the value for mbean for an
   * attribute is null then return 0 as default value else return the attribute
   * value
   */
  private Long getLongAttribute(Object object, String name) {
    try {
      if (!(object.getClass().equals(Long.class))) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info("************************Unexpected type for attribute: "
              + name + " Expected type: " + Long.class.getName()
              + " Received type: " + object.getClass().getName()
              + "************************");
        }
        return Long.valueOf(0);
      } else {
        return (Long) object;
      }
    } catch (Exception e) {
      if (LOGGER.infoEnabled()) {
        LOGGER.info("Exception Occured: " + e.getMessage());
      }
      return Long.valueOf(0);
    }

  }

  /**
   * function used to handle String data type if the value for mbean for an
   * attribute is null then return the empty string as default value else return
   * the attribute value
   */
  private String getStringAttribute(Object object, String name) {
    try {
      if (!(object.getClass().equals(String.class))) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info("************************Unexpected type for attribute: "
              + name + " Expected type: " + String.class.getName()
              + " Received type: " + object.getClass().getName()
              + "************************");
        }
        return "";
      } else {
        return (String) object;
      }
    } catch (Exception e) {
      if (LOGGER.infoEnabled()) {
        LOGGER.info("Exception Occured: " + e.getMessage());
      }
      return "";
    }
  }

  /**
   * function used to handle Boolean data type if the value for mbean for an
   * attribute is null then return false as default value else return the
   * attribute value
   */
  private Boolean getBooleanAttribute(Object object, String name) {
    try {
      if (!(object.getClass().equals(Boolean.class))) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info("************************Unexpected type for attribute: "
              + name + " Expected type: " + Boolean.class.getName()
              + " Received type: " + object.getClass().getName()
              + "************************");
        }
        return Boolean.FALSE;
      } else {
        return (Boolean) object;
      }
    } catch (Exception e) {
      if (LOGGER.infoEnabled()) {
        LOGGER.info("Exception Occured: " + e.getMessage());
      }
      return Boolean.FALSE;
    }
  }

  /**
   * function used to handle Double data type if the value for mbean for an
   * attribute is null then return 0.0 as default value else return the
   * attribute value
   */
  private Double getDoubleAttribute(Object object, String name) {
    try {
      if (!(object.getClass().equals(Double.class))) {
        if (LOGGER.infoEnabled()) {
          LOGGER.info("************************Unexpected type for attribute: "
              + name + " Expected type: " + Double.class.getName()
              + " Received type: " + object.getClass().getName()
              + "************************");
        }
        return Double.valueOf(0);
      } else {
        return (Double) object;
      }
    } catch (Exception e) {
      if (LOGGER.infoEnabled()) {
        LOGGER.info("Exception Occured: " + e.getMessage());
      }
      return Double.valueOf(0);
    }
  }

  /**
   * function used to get attribute values of Member Region and map them to
   * Member vo
   *
   * @param mbeanName
   *          Member Region MBean
   */
  private void updateMemberRegion(ObjectName mbeanName) throws IOException {

    try {
      String memberName = mbeanName
          .getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

      Cluster.Member member = cluster.getMembersHMap().get(memberName);

      AttributeList attributeList = this.mbs.getAttributes(mbeanName,
          PulseConstants.REGION_MBEAN_ATTRIBUTES);

      // retrieve the full path of the region
      String regionFullPathKey = null;
      for (int i = 0; i < attributeList.size(); i++) {
        Attribute attribute = (Attribute) attributeList.get(i);

        if (attribute.getName().equals(PulseConstants.MBEAN_ATTRIBUTE_FULLPATH)) {
          regionFullPathKey = getStringAttribute(attribute.getValue(),
              attribute.getName());
          break;
        }
      }

      // if member does not exists defined for this region then create a member
      if (null == member) {
        member = new Cluster.Member();
        member.setName(memberName);
        cluster.getMembersHMap().put(memberName, member);
      }

      // if region with given path exists then update same else add new region
      Cluster.Region region = member.getMemberRegions().get(regionFullPathKey);
      if (null == region) {
        region = new Cluster.Region();
        member.getMemberRegions().put(regionFullPathKey, region);
        member.setTotalRegionCount(member.getTotalRegionCount() + 1);
      }
      region.setFullPath(regionFullPathKey); // use already retrieved values

      // update the existing or new region
      for (int i = 0; i < attributeList.size(); i++) {
        Attribute attribute = (Attribute) attributeList.get(i);

        if (attribute.getName().equals(PulseConstants.MBEAN_ATTRIBUTE_FULLPATH)) {
          region.setFullPath(getStringAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_DISKREADSRATE)) {
          region.setDiskReadsRate(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_DISKWRITESRATE)) {
          region.setDiskWritesRate(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_GETSRATE)) {
          region.setGetsRate(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_LRUEVICTIONRATE)) {
          region.setLruEvictionRate(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_PUTSRATE)) {
          region.setPutsRate(getFloatAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_REGIONTYPE)) {
          region.setRegionType(getStringAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_MEMBERCOUNT)) {
          region.setMemberCount(getIntegerAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_ENTRYSIZE)) {
          region.setEntrySize(getLongAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_ENTRYCOUNT)) {
          region.setSystemRegionEntryCount(getLongAttribute(
              attribute.getValue(), attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_NAME)) {
          region.setName(getStringAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_PERSISTENTENABLED)) {
          region.setPersistentEnabled(getBooleanAttribute(attribute.getValue(),
              attribute.getName()));
        } else if (attribute.getName().equals(
            PulseConstants.MBEAN_ATTRIBUTE_GATEWAYENABLED)) {
          region.setWanEnabled(getBooleanAttribute(attribute.getValue(),
              attribute.getName()));
        }
      }
      /* GemfireXD related code
      try{// Added for Rolling upgrade changes. Needs to removed once Rolling upgrade handled gracefully
    	  CompositeData compositeData = (CompositeData) (this.mbs.invoke(mbeanName,
    	          PulseConstants.MBEAN_OPERATION_LISTREGIONATTRIBUTES, null, null));

    	      if (compositeData != null) {
    	        if (compositeData.containsKey(PulseConstants.COMPOSITE_DATA_KEY_SCOPE)) {
    	          region.setScope((String) compositeData
    	              .get(PulseConstants.COMPOSITE_DATA_KEY_SCOPE));
    	        } else if (compositeData
    	            .containsKey(PulseConstants.COMPOSITE_DATA_KEY_DISKSTORENAME)) {
    	          region.setDiskStoreName((String) compositeData
    	              .get(PulseConstants.COMPOSITE_DATA_KEY_DISKSTORENAME));
    	        } else if (compositeData
    	            .containsKey(PulseConstants.COMPOSITE_DATA_KEY_DISKSYNCHRONOUS)) {
    	          region.setDiskSynchronous((Boolean) compositeData
    	              .get(PulseConstants.COMPOSITE_DATA_KEY_DISKSYNCHRONOUS));
    	        }
    	      }
      }catch (MBeanException anfe) {
          LOGGER.warning(anfe);
      }catch (javax.management.RuntimeMBeanException anfe) {
          region.setScope("");
          region.setDiskStoreName("");
          region.setDiskSynchronous(false);
          //LOGGER.warning("Some of the Pulse elements are not available currently. There might be a GemFire upgrade going on.");
      }
     

      // Remove deleted regions from member's regions list
      for (Iterator<String> it = cluster.getDeletedRegions().iterator(); it
          .hasNext();) {
        String deletedRegion = it.next();
        if (member.getMemberRegions().get(deletedRegion) != null) {
          member.getMemberRegions().remove(deletedRegion);
        }
        member.setTotalRegionCount(member.getMemberRegions().size());
      }*/
    } catch (InstanceNotFoundException infe) {
      LOGGER.warning(infe);
    } catch (ReflectionException re) {
      LOGGER.warning(re);
    } 
  }

  /**
   * System Notification Listener
   */
  @Override
  public void handleNotification(Notification notification, Object handback) {
    String type = notification.getType();

    if (PulseConstants.NOTIFICATION_TYPE_SYSTEM_ALERT.equals(type)) {
      Cluster.Alert alert = new Cluster.Alert();
      Long timeStamp = notification.getTimeStamp();
      Date date = new Date(timeStamp);
      alert.setTimestamp(date);
      String notificationSource = (String) notification.getUserData();
      alert.setMemberName(notificationSource);
      String alertDescription = notification.getMessage();
      if (alertDescription.startsWith("[error")) {
        alert.setSeverity(Cluster.Alert.ERROR);
      } else if (alertDescription.startsWith("[warning")) {
        alert.setSeverity(Cluster.Alert.WARNING);
      }else if(alertDescription.startsWith("[severe")) {
        alert.setSeverity(Cluster.Alert.SEVERE);
      } else {
        alert.setSeverity(Cluster.Alert.INFO);
      }
      alert.setDescription(notification.getMessage());
      alert.setAcknowledged(false);
      alert.setId(Cluster.Alert.nextID());
      cluster.addAlert(alert);
    }else{
      Cluster.Alert alert = new Cluster.Alert();
      Long timeStamp = notification.getTimeStamp();
      Date date = new Date(timeStamp);
      alert.setTimestamp(date);
      String notificationSource = (String) notification.getSource();
      alert.setMemberName(notificationSource);
      String alertDescription = notification.getMessage();
      alert.setDescription(alertDescription);

      alert.setSeverity(Cluster.Alert.INFO);

      alert.setAcknowledged(false);
      alert.setId(Cluster.Alert.nextID());
      cluster.addAlert(alert);

      if(PulseConstants.NOTIFICATION_TYPE_REGION_DESTROYED.equals(type)){
        // Remove deleted region from member's regions list
        String msg = notification.getMessage();
        String deletedRegion = msg.substring(msg.indexOf("Name ")+"Name ".length());
        String memberName = notificationSource;
        Cluster.Member member = cluster.getMembersHMap().get(memberName);

        if (member.getMemberRegions().get(deletedRegion) != null) {
          member.getMemberRegions().remove(deletedRegion);
          member.setTotalRegionCount(member.getMemberRegions().size());
        }
      }
    }
  }

  @Override
  public JSONObject executeQuery(String queryText, String members, int limit)
      throws JSONException {

    JSONObject queryResult = new JSONObject();

    if (this.mbs != null && this.systemMBeans != null) {

      Object opParams[] = { queryText, members, limit };

      for (ObjectName sysMBean : this.systemMBeans) {
        try {
          String resultString = (String) (this.mbs.invoke(sysMBean,
              PulseConstants.MBEAN_OPERATION_QUERYDATABROWSER, opParams,
              this.opSignature));

          // Convert result into JSON
          queryResult = new JSONObject(resultString);

        } catch (Exception e) {
          // Send error into result
          queryResult.put("error", e.getMessage());
          if (LOGGER.fineEnabled()) {
            LOGGER.fine(e.getMessage());
          }
        }
      }
    }

    return queryResult;
  }

  // Function to find out number of current JVM pauses on cluster or Member
  private long determineCurrentJVMPauses(String type, String key,
      long totalJVMPauses) {

    long currentJVMPausesCount = 0L;

    if (type.equalsIgnoreCase(PulseConstants.JVM_PAUSES_TYPE_CLUSTER)) {

      long prevJVMPausesCount = cluster.getPreviousJVMPauseCount();

      if (totalJVMPauses > prevJVMPausesCount) {
        currentJVMPausesCount = totalJVMPauses - prevJVMPausesCount;
      }
      cluster.setPreviousJVMPauseCount(totalJVMPauses);

    } else {

      Cluster.Member clusterMember = cluster.getMembersHMap().get(key);

      if (clusterMember != null) {
        long prevJVMPausesCount = clusterMember.getPreviousJVMPauseCount();

        if (totalJVMPauses > prevJVMPausesCount) {
          currentJVMPausesCount = totalJVMPauses - prevJVMPausesCount;
        }
        clusterMember.setPreviousJVMPauseCount(totalJVMPauses);
      }
    }

    return currentJVMPausesCount;
  }

  // Method which finds and return GemFire Version from argument passed
  private String obtainGemfireVersion(String version) {

    final String versionText = "Java version:   ";
    String gemfireVersion = "";
    int startIndex = version.indexOf(versionText) + versionText.length();
    gemfireVersion = version.substring(startIndex);
    gemfireVersion = gemfireVersion.substring(0, gemfireVersion.indexOf(" "));

    return gemfireVersion;
  }

}
