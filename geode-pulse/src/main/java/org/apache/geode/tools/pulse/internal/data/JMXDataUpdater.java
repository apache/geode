/*
 *
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
 *
 */

package org.apache.geode.tools.pulse.internal.data;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
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
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.geode.tools.pulse.internal.data.JmxManagerFinder.JmxManagerInfo;

/**
 * Class JMXDataUpdater Class used for creating JMX connection and getting all the required MBeans
 *
 * @since GemFire version 7.0.Beta 2012-09-23
 */
public class JMXDataUpdater implements IClusterUpdater, NotificationListener {

  private static final Logger logger = LogManager.getLogger();
  private final ResourceBundle resourceBundle;
  private final Repository repository;

  private JMXConnector conn = null;
  private MBeanServerConnection mbs = null;
  private final String serverName;
  private final String port;
  private Boolean isAddedNotiListner = false;
  private final Cluster cluster;

  // MBean object names
  private ObjectName MBEAN_OBJECT_NAME_SYSTEM_DISTRIBUTED;
  private ObjectName MBEAN_OBJECT_NAME_REGION_DISTRIBUTED;
  private ObjectName MBEAN_OBJECT_NAME_MEMBER;
  private ObjectName MBEAN_OBJECT_NAME_STATEMENT_DISTRIBUTED;

  private Set<ObjectName> systemMBeans = null;

  private final String[] opSignature =
      {String.class.getName(), String.class.getName(), int.class.getName()};

  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * constructor used for creating JMX connection
   */
  public JMXDataUpdater(String server, String port, Cluster cluster, ResourceBundle resourceBundle,
      Repository repository) {
    serverName = server;
    this.port = port;
    this.cluster = cluster;
    this.resourceBundle = resourceBundle;
    this.repository = repository;

    try {
      // Initialize MBean object names
      MBEAN_OBJECT_NAME_SYSTEM_DISTRIBUTED =
          new ObjectName(PulseConstants.OBJECT_NAME_SYSTEM_DISTRIBUTED);
      MBEAN_OBJECT_NAME_REGION_DISTRIBUTED =
          new ObjectName(PulseConstants.OBJECT_NAME_REGION_DISTRIBUTED);
      MBEAN_OBJECT_NAME_MEMBER = new ObjectName(PulseConstants.OBJECT_NAME_MEMBER);
      MBEAN_OBJECT_NAME_STATEMENT_DISTRIBUTED =
          new ObjectName(PulseConstants.OBJECT_NAME_STATEMENT_DISTRIBUTED);

    } catch (MalformedObjectNameException | NullPointerException e) {
      logger.fatal(e);
    }
  }

  private JmxManagerInfo getManagerInfoFromLocator(Repository repository) {

    try {
      String locatorHost = repository.getHost();
      int locatorPort = Integer.parseInt(repository.getPort());

      logger.info("{}={} & {}={}", resourceBundle.getString("LOG_MSG_HOST"), locatorHost,
          resourceBundle.getString("LOG_MSG_PORT"), locatorPort);

      InetAddress inetAddr = InetAddress.getByName(locatorHost);

      if ((inetAddr instanceof Inet4Address) || (inetAddr instanceof Inet6Address)) {

        if (inetAddr instanceof Inet4Address) {
          logger.info("{}: {}", resourceBundle.getString("LOG_MSG_LOCATOR_IPV4_ADDRESS"), inetAddr);
        } else {
          logger.info("{}: {}", resourceBundle.getString("LOG_MSG_LOCATOR_IPV6_ADDRESS"), inetAddr);
        }

        JmxManagerInfo jmxManagerInfo = JmxManagerFinder.askLocatorForJmxManager(inetAddr,
            locatorPort, 15000, repository.isUseSSLLocator());

        if (jmxManagerInfo.port == 0) {
          logger.info(resourceBundle.getString("LOG_MSG_LOCATOR_COULD_NOT_FIND_MANAGER"));
        }
        return jmxManagerInfo;
      } else {
        // Locator has Invalid locator Address
        logger.info(resourceBundle.getString("LOG_MSG_LOCATOR_BAD_ADDRESS"));
        cluster
            .setConnectionErrorMsg(resourceBundle.getString("LOG_MSG_JMX_CONNECTION_BAD_ADDRESS"));
        // update message to display on UI
        cluster
            .setConnectionErrorMsg(resourceBundle.getString("LOG_MSG_JMX_CONNECTION_BAD_ADDRESS"));
        return null;
      }
    } catch (IOException e) {
      StringWriter swBuffer = new StringWriter();
      PrintWriter prtWriter = new PrintWriter(swBuffer);
      e.printStackTrace(prtWriter);
      logger.fatal("Exception Details : {}\n", swBuffer);
    }
    return null;
  }

  /**
   * Get the jmx connection
   */
  @Override
  public JMXConnector connect(Object credentials) {
    try {

      String jmxSerURL = "";

      logger.info("{}:{}", resourceBundle.getString("LOG_MSG_USE_LOCATOR_VALUE"),
          repository.getJmxUseLocator());

      if (repository.getJmxUseLocator()) {
        JmxManagerInfo jmxManagerInfo = getManagerInfoFromLocator(repository);

        if (jmxManagerInfo.port == 0) {
          logger.info(resourceBundle.getString("LOG_MSG_LOCATOR_COULD_NOT_FIND_MANAGER"));
        } else {
          logger.info("{}: {}={} & {}={}, {}",
              resourceBundle.getString("LOG_MSG_LOCATOR_FOUND_MANAGER"),
              resourceBundle.getString("LOG_MSG_HOST"), jmxManagerInfo.host,
              resourceBundle.getString("LOG_MSG_PORT"), jmxManagerInfo.port,
              (jmxManagerInfo.ssl ? resourceBundle.getString("LOG_MSG_WITH_SSL")
                  : resourceBundle.getString("LOG_MSG_WITHOUT_SSL")));

          jmxSerURL =
              formJMXServiceURLString(jmxManagerInfo.host, String.valueOf(jmxManagerInfo.port));
        }
      } else {
        logger.info("{}={} & {}={}", resourceBundle.getString("LOG_MSG_HOST"), serverName,
            resourceBundle.getString("LOG_MSG_PORT"), port);
        jmxSerURL = formJMXServiceURLString(serverName, port);
      }

      if (StringUtils.isNotBlank(jmxSerURL)) {
        JMXServiceURL url = new JMXServiceURL(jmxSerURL);
        Map<String, Object> env = new HashMap<>();
        env.put(JMXConnector.CREDENTIALS, credentials);

        Properties originalProperties = System.getProperties();
        try {
          Properties updatedProperties = new Properties(originalProperties);
          if (repository.isUseSSLManager()) {
            for (String sslProperty : repository.getJavaSslProperties().stringPropertyNames()) {
              updatedProperties.setProperty(sslProperty,
                  repository.getJavaSslProperties().getProperty(sslProperty));
            }

            System.setProperties(updatedProperties);
            env.put("com.sun.jndi.rmi.factory.socket", new SslRMIClientSocketFactory());
          }
          logger.info("Connecting to jmxURL : {}", jmxSerURL);
          conn = JMXConnectorFactory.connect(url, env);
          mbs = conn.getMBeanServerConnection();
          cluster.setConnectedFlag(true);
        } finally {
          System.setProperties(originalProperties);
        }
      }
    } catch (Exception e) {
      cluster.setConnectedFlag(false);
      cluster.setConnectionErrorMsg(e.getMessage());
      logger.fatal(e.getMessage(), e);
      if (conn != null) {
        try {
          conn.close();
        } catch (Exception e1) {
          logger.fatal(e1.getMessage(), e1);
        }
        conn = null;
      }
    }
    return conn;
  }

  private String formJMXServiceURLString(String host, String port) throws UnknownHostException {
    /*
     * String jmxSerURL = "service:jmx:rmi://" + serverName + "/jndi/rmi://" + serverName + ":" +
     * port + "/jmxrmi";
     */
    String jmxSerURL = "";
    if (host.equalsIgnoreCase("localhost")) {
      // Create jmx service url for 'localhost'
      jmxSerURL = "service:jmx:rmi://" + host + "/jndi/rmi://" + host + ":" + port + "/jmxrmi";
    } else {
      InetAddress inetAddr = InetAddress.getByName(host);
      if (inetAddr instanceof Inet4Address) {
        // Create jmx service url for IPv4 address
        jmxSerURL = "service:jmx:rmi://" + host + "/jndi/rmi://" + host + ":" + port + "/jmxrmi";
      } else if (inetAddr instanceof Inet6Address) {
        // Create jmx service url for IPv6 address
        jmxSerURL =
            "service:jmx:rmi://[" + host + "]/jndi/rmi://[" + host + "]:" + port + "/jmxrmi";
      }
    }

    return jmxSerURL;
  }

  /**
   * function used for updating Cluster Data.
   */
  @Override
  public boolean updateData() {
    try {
      if (conn == null) {
        return false;
      }

      // deleted Members
      cluster.getDeletedMembers().clear();
      for (Entry<String, Cluster.Member> memberSet : cluster.getMembersHMap().entrySet()) {
        cluster.getDeletedMembers().add(memberSet.getKey());
      }

      // Deleted Regions
      cluster.getDeletedRegions().clear();
      for (Cluster.Region region : cluster.getClusterRegions().values()) {
        cluster.getDeletedRegions().add(region.getFullPath());
      }

      systemMBeans = mbs.queryNames(MBEAN_OBJECT_NAME_SYSTEM_DISTRIBUTED, null);
      for (ObjectName sysMBean : systemMBeans) {
        updateClusterSystem(sysMBean);
      }

      // Cluster Regions/Tables
      Set<ObjectName> regionMBeans =
          mbs.queryNames(MBEAN_OBJECT_NAME_REGION_DISTRIBUTED, null);

      // For Gemfire
      for (ObjectName regMBean : regionMBeans) {
        updateClusterRegion(regMBean);
      }

      // Remove deleted regions from cluster's regions list
      for (String s : cluster.getDeletedRegions()) {
        cluster.removeClusterRegion(s);
      }

      List<ObjectName> serviceMBeans = new ArrayList<>();
      List<ObjectName> nonServiceMBeans = new ArrayList<>();

      Set<ObjectName> memberMBeans = mbs.queryNames(MBEAN_OBJECT_NAME_MEMBER, null);
      for (ObjectName mBean : memberMBeans) {
        String service = mBean.getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_SERVICE);
        if (service == null) {
          nonServiceMBeans.add(mBean);
        } else {
          serviceMBeans.add(mBean);
        }
      }

      // Make sure that we process 'pure' members first. This ensures that various structures in
      // Cluster are set up correctly since they are keyed on the 'host' attribute which does not
      // necessarily appear in other MBeans. This avoids the possibility of having a 'null' host
      // icon appear in the Pulse UI.
      for (ObjectName mBean : nonServiceMBeans) {
        updateClusterMember(mBean);
      }

      for (ObjectName serviceMBean : serviceMBeans) {
        String service = serviceMBean.getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_SERVICE);
        switch (service) {
          case PulseConstants.MBEAN_KEY_PROPERTY_SERVICE_VALUE_REGION:
            updateMemberRegion(serviceMBean);
            break;
          case PulseConstants.MBEAN_KEY_PROPERTY_SERVICE_VALUE_CACHESERVER:
            updateMemberClient(serviceMBean);
            break;
          case PulseConstants.MBEAN_KEY_PROPERTY_SERVICE_VALUE_GATEWAYRECEIVER:
            updateGatewayReceiver(serviceMBean);
            break;
          case PulseConstants.MBEAN_KEY_PROPERTY_SERVICE_VALUE_GATEWAYSENDER:
            updateGatewaySender(serviceMBean);
            break;
          case PulseConstants.MBEAN_KEY_PROPERTY_SERVICE_VALUE_ASYNCEVENTQUEUE:
            updateAsyncEventQueue(serviceMBean);
            break;
          case PulseConstants.MBEAN_KEY_PROPERTY_SERVICE_VALUE_LOCATOR:
            updateClusterMember(serviceMBean);
            break;
        }
      }

      // Cluster Query Statistics
      Set<ObjectName> statementObjectNames =
          mbs.queryNames(MBEAN_OBJECT_NAME_STATEMENT_DISTRIBUTED, null);
      for (ObjectName stmtObjectName : statementObjectNames) {
        updateClusterStatement(stmtObjectName);
      }
    } catch (IOException ioe) {
      logger.fatal(ioe.getMessage(), ioe);
      mbs = null;
      if (conn != null) {
        try {
          conn.close();
        } catch (IOException e1) {
          logger.fatal(e1.getMessage(), e1);
        }
        conn = null;
        cluster.setConnectedFlag(false);
        cluster.setConnectionErrorMsg(ioe.getMessage());
      }

      return false;
    }

    // If there were members deleted, remove them from the membersList &
    // physicalToMember.
    for (String memberKey : cluster.getDeletedMembers()) {
      if (cluster.getMembersHMap().containsKey(memberKey)) {
        Cluster.Member member = cluster.getMembersHMap().get(memberKey);
        List<Cluster.Member> memberArrList = cluster.getPhysicalToMember().get(member.getHost());
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
   * function used to get attribute values of Cluster System and map them to cluster vo
   *
   * @param mbeanName Cluster System MBean
   */
  private void updateClusterSystem(ObjectName mbeanName) throws IOException {
    try {
      if (!isAddedNotiListner) {
        mbs.addNotificationListener(mbeanName, this, null, new Object());
        isAddedNotiListner = true;
      }

      String[] serverCnt = (String[]) (mbs.invoke(mbeanName,
          PulseConstants.MBEAN_OPERATION_LISTSERVERS, null, null));
      cluster.setServerCount(serverCnt.length);

      TabularData table = (TabularData) (mbs.invoke(mbeanName,
          PulseConstants.MBEAN_OPERATION_VIEWREMOTECLUSTERSTATUS, null, null));

      @SuppressWarnings("unchecked")
      Collection<CompositeData> rows = (Collection<CompositeData>) table.values();
      cluster.getWanInformationObject().clear();
      for (CompositeData row : rows) {
        final Object key = row.get("key");
        final Object value = row.get("value");
        cluster.getWanInformationObject().put((String) key, (Boolean) value);
      }

      AttributeList attributeList =
          mbs.getAttributes(mbeanName, PulseConstants.CLUSTER_MBEAN_ATTRIBUTES);

      for (Object o : attributeList) {

        Attribute attribute = (Attribute) o;
        String name = attribute.getName();
        switch (name) {
          case PulseConstants.MBEAN_ATTRIBUTE_MEMBERCOUNT:
            cluster.setMemberCount(getIntegerAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_NUMCLIENTS:
            cluster.setClientConnectionCount(
                getIntegerAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_DISTRIBUTEDSYSTEMID:
            cluster.setClusterId(getIntegerAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_LOCATORCOUNT:
            cluster.setLocatorCount(getIntegerAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_NUMRUNNIGFUNCTION:
            try {
              cluster.setRunningFunctionCount(
                  getIntegerAttribute(attribute.getValue(), attribute.getName()));
            } catch (Exception e) {
              cluster.setRunningFunctionCount(0);
              continue;
            }
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_REGISTEREDCQCOUNT:
            cluster
                .setRegisteredCQCount(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_NUMSUBSCRIPTIONS:
            cluster.setSubscriptionCount(
                getIntegerAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_NUMTXNCOMMITTED:
            cluster.setTxnCommittedCount(
                getIntegerAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_NUMTXNROLLBACK:
            cluster.setTxnRollbackCount(
                getIntegerAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_TOTALHEAPSIZE:
            cluster.setTotalHeapSize(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_USEDHEAPSIZE:
            try {
              cluster.setUsedHeapSize(getLongAttribute(attribute.getValue(), attribute.getName()));
            } catch (Exception e) {
              cluster.setUsedHeapSize(0);
              continue;
            }
            cluster.getMemoryUsageTrend().add(cluster.getUsedHeapSize());
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_TOTALREGIONENTRYCOUNT:
            cluster.setTotalRegionEntryCount(
                getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_CURRENTENTRYCOUNT:
            cluster.setCurrentQueryCount(
                getIntegerAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_TOTALDISKUSAGE:
            try {
              cluster
                  .setTotalBytesOnDisk(getLongAttribute(attribute.getValue(), attribute.getName()));
            } catch (Exception e) {
              cluster.setTotalBytesOnDisk(0);
              continue;
            }
            cluster.getTotalBytesOnDiskTrend().add(cluster.getTotalBytesOnDisk());
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_DISKWRITESRATE:
            cluster
                .setDiskWritesRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            cluster.getThroughoutWritesTrend().add(cluster.getDiskWritesRate());
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_AVERAGEWRITES:
            try {
              cluster.setWritePerSec(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            } catch (Exception e) {
              cluster.setWritePerSec(0);
              continue;
            }
            cluster.getWritePerSecTrend().add(cluster.getWritePerSec());
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_AVERAGEREADS:
            try {
              cluster.setReadPerSec(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            } catch (Exception e) {
              cluster.setReadPerSec(0);
              continue;
            }
            cluster.getReadPerSecTrend().add(cluster.getReadPerSec());
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_QUERYREQUESTRATE:
            cluster.setQueriesPerSec(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            cluster.getQueriesPerSecTrend().add(cluster.getQueriesPerSec());
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_DISKREADSRATE:
            cluster.setDiskReadsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            cluster.getThroughoutReadsTrend().add(cluster.getDiskReadsRate());
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_JVMPAUSES:
            long trendVal = determineCurrentJVMPauses(PulseConstants.JVM_PAUSES_TYPE_CLUSTER, "",
                getLongAttribute(attribute.getValue(), attribute.getName()));
            cluster.setGarbageCollectionCount(trendVal);
            cluster.getGarbageCollectionTrend().add(cluster.getGarbageCollectionCount());
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_TOTALREGIONCOUNT:
            cluster.setTotalRegionCount(
                getIntegerAttribute(attribute.getValue(), attribute.getName()));
            break;
        }
      }

    } catch (InstanceNotFoundException | ReflectionException | MBeanException infe) {
      logger.warn(infe);
    }
  }

  /**
   * function used to get attribute values of Gateway Receiver and map them to GatewayReceiver inner
   * class object
   *
   * @return GatewayReceiver object
   */
  private Cluster.GatewayReceiver initGatewayReceiver(ObjectName mbeanName)
      throws InstanceNotFoundException, ReflectionException, IOException {

    Cluster.GatewayReceiver gatewayReceiver = new Cluster.GatewayReceiver();

    AttributeList attributeList =
        mbs.getAttributes(mbeanName, PulseConstants.GATEWAY_MBEAN_ATTRIBUTES);

    for (Object o : attributeList) {
      Attribute attribute = (Attribute) o;

      switch (attribute.getName()) {
        case PulseConstants.MBEAN_ATTRIBUTE_PORT:
          gatewayReceiver
              .setListeningPort(getIntegerAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_EVENTRECEIVEDDATE:
          gatewayReceiver
              .setLinkThroughput(getDoubleAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_AVEARGEBATCHPROCESSINGTIME:
          gatewayReceiver
              .setAvgBatchProcessingTime(
                  getLongAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_RUNNING:
          gatewayReceiver.setStatus(getBooleanAttribute(attribute.getValue(), attribute.getName()));
          break;
      }
    }
    return gatewayReceiver;
  }

  /**
   * function used to get attribute values of Gateway Sender and map them to GatewaySender inner
   * class object
   */
  private Cluster.GatewaySender initGatewaySender(ObjectName mbeanName)
      throws InstanceNotFoundException, ReflectionException, IOException {

    Cluster.GatewaySender gatewaySender = new Cluster.GatewaySender();
    AttributeList attributeList =
        mbs.getAttributes(mbeanName, PulseConstants.GATEWAYSENDER_MBEAN_ATTRIBUTES);

    for (Object o : attributeList) {
      Attribute attribute = (Attribute) o;
      String name = attribute.getName();
      switch (name) {
        case PulseConstants.MBEAN_ATTRIBUTE_EVENTRECEIVEDDATE:
          gatewaySender
              .setLinkThroughput(getDoubleAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_BATCHSIZE:
          gatewaySender
              .setBatchSize(getIntegerAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_SENDERID:
          gatewaySender.setId(getStringAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_EVENTQUEUESIZE:
          gatewaySender
              .setQueueSize(getIntegerAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_RUNNING:
          gatewaySender.setStatus(getBooleanAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_PRIMARY:
          gatewaySender.setPrimary(getBooleanAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_PERSISTENCEENABLED:
          gatewaySender.setPersistenceEnabled(
              getBooleanAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_PARALLEL:
          gatewaySender
              .setSenderType(getBooleanAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_REMOTE_DS_ID:
          gatewaySender
              .setRemoteDSId(getIntegerAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_EVENTS_EXCEEDING_ALERT_THRESHOLD:
          gatewaySender.setEventsExceedingAlertThreshold(
              getIntegerAttribute(attribute.getValue(), attribute.getName()));
          break;
      }
    }
    return gatewaySender;
  }

  /**
   * function used for getting list of Gateway Senders from mBean for giving member and update the
   * list of gateway senders for respective member object
   */
  private void updateGatewaySender(ObjectName mbeanName) throws IOException {

    try {
      String memberName = mbeanName.getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

      if (cluster.getMembersHMap().containsKey(memberName)) {
        Cluster.Member existingMember = cluster.getMembersHMap().get(memberName);
        Cluster.GatewaySender gatewaySender = initGatewaySender(mbeanName);
        for (Iterator<Cluster.GatewaySender> it =
            existingMember.getGatewaySenderList().iterator(); it.hasNext();) {
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
    } catch (InstanceNotFoundException | ReflectionException infe) {
      logger.warn(infe);
    }
  }

  /**
   * function used to get attribute values of Async Event Queue and map them to Async Event Queue
   * inner class object
   */
  private Cluster.AsyncEventQueue initAsyncEventQueue(ObjectName mbeanName)
      throws InstanceNotFoundException, ReflectionException, IOException {

    Cluster.AsyncEventQueue asyncEventQueue = new Cluster.AsyncEventQueue();
    AttributeList attributeList =
        mbs.getAttributes(mbeanName, PulseConstants.ASYNC_EVENT_QUEUE_MBEAN_ATTRIBUTES);

    for (Object o : attributeList) {
      Attribute attribute = (Attribute) o;
      String name = attribute.getName();
      switch (name) {
        case PulseConstants.MBEAN_ATTRIBUTE_AEQ_ASYNCEVENTID:
          asyncEventQueue.setId(getStringAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_AEQ_ASYNC_EVENT_LISTENER:
          asyncEventQueue
              .setAsyncEventListener(getStringAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_AEQ_BATCH_CONFLATION_ENABLED:
          asyncEventQueue.setBatchConflationEnabled(
              getBooleanAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_AEQ_BATCH_TIME_INTERVAL:
          asyncEventQueue
              .setBatchTimeInterval(getLongAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_AEQ_BATCH_SIZE:
          asyncEventQueue
              .setBatchSize(getIntegerAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_AEQ_EVENT_QUEUE_SIZE:
          asyncEventQueue
              .setEventQueueSize(getIntegerAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_AEQ_PARALLEL:
          asyncEventQueue
              .setParallel(getBooleanAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_AEQ_PRIMARY:
          asyncEventQueue
              .setPrimary(getBooleanAttribute(attribute.getValue(), attribute.getName()));
          break;
      }
    }
    return asyncEventQueue;
  }

  /**
   * function used for getting list of Gateway Senders from mBean for giving member and update the
   * list of gateway senders for respective member object
   */
  private void updateAsyncEventQueue(ObjectName mbeanName) throws IOException {
    try {
      String memberName = mbeanName.getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

      if (cluster.getMembersHMap().containsKey(memberName)) {
        Cluster.Member existingMember = cluster.getMembersHMap().get(memberName);
        Cluster.AsyncEventQueue asyncQ = initAsyncEventQueue(mbeanName);
        for (Iterator<Cluster.AsyncEventQueue> it =
            existingMember.getAsyncEventQueueList().iterator(); it.hasNext();) {
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
    } catch (InstanceNotFoundException | ReflectionException infe) {
      logger.warn(infe);
    }
  }

  /**
   * function used for getting a Gateway Receiver from mBean for giving member and update the
   * gateway receiver for respective member object
   */
  private void updateGatewayReceiver(ObjectName mbeanName) throws IOException {

    try {
      String memberName = mbeanName.getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

      if (cluster.getMembersHMap().containsKey(memberName)) {
        Cluster.Member existingMember = cluster.getMembersHMap().get(memberName);
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
    } catch (InstanceNotFoundException | ReflectionException infe) {
      logger.warn(infe);
    }
  }

  /**
   * function used for getting member clients from mbean and update the clients information in
   * member object's client arraylist
   */
  private void updateMemberClient(ObjectName mbeanName) throws IOException {

    try {
      String memberName = mbeanName.getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

      if (cluster.getMembersHMap().containsKey(memberName)) {
        Cluster.Member existingMember = cluster.getMembersHMap().get(memberName);
        HashMap<String, Cluster.Client> memberClientsHM = new HashMap<>();

        existingMember.setMemberPort(
            "" + mbs.getAttribute(mbeanName, PulseConstants.MBEAN_ATTRIBUTE_PORT));

        mbs.getAttribute(mbeanName, PulseConstants.MBEAN_ATTRIBUTE_HOSTNAMEFORCLIENTS_ALT);
        existingMember.setHostnameForClients((String) mbs.getAttribute(mbeanName,
            PulseConstants.MBEAN_ATTRIBUTE_HOSTNAMEFORCLIENTS_ALT));
        existingMember.setBindAddress(
            (String) mbs.getAttribute(mbeanName, PulseConstants.MBEAN_ATTRIBUTE_BINDADDRESS));

        CompositeData[] compositeData = (CompositeData[]) (mbs.invoke(mbeanName,
            PulseConstants.MBEAN_OPERATION_SHOWALLCLIENTS, null, null));
        for (CompositeData cmd : compositeData) {
          Cluster.Client client = new Cluster.Client();
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_CLIENTID)) {
            client.setId((String) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_CLIENTID));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_NAME)) {
            client.setName((String) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_NAME));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_HOSTNAME)) {
            client.setHost((String) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_HOSTNAME));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_QUEUESIZE)) {
            client.setQueueSize((Integer) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_QUEUESIZE));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_PROCESSCPUTIME)) {
            client.setProcessCpuTime(
                (Long) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_PROCESSCPUTIME));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_UPTIME)) {
            client.setUptime((Long) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_UPTIME));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_NUMOFTHREADS)) {
            client.setThreads((Integer) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_NUMOFTHREADS));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_NUMOFGETS)) {
            client.setGets((Integer) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_NUMOFGETS));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_NUMOFPUTS)) {
            client.setPuts((Integer) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_NUMOFPUTS));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_CPUS)) {
            client.setCpus((Integer) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_CPUS));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_CPUS)) {
            client.setCpuUsage(0);
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_CONNECTED)) {
            client.setConnected((Boolean) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_CONNECTED));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_CLIENTCQCOUNT)) {
            client.setClientCQCount(
                (Integer) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_CLIENTCQCOUNT));
          }
          if (cmd.containsKey(PulseConstants.COMPOSITE_DATA_KEY_SUBSCRIPTIONENABLED)) {
            client.setSubscriptionEnabled(
                (Boolean) cmd.get(PulseConstants.COMPOSITE_DATA_KEY_SUBSCRIPTIONENABLED));
          }
          memberClientsHM.put(client.getId(), client);
        }
        existingMember.updateMemberClientsHMap(memberClientsHM);
      }
    } catch (InstanceNotFoundException | ReflectionException | AttributeNotFoundException
        | MBeanException infe) {
      logger.warn(infe);
    }
  }

  /**
   * Add member specific region information on the region
   *
   * @param regionObjectName used to construct the jmx objectname. For region name that has special
   *        characters in, it will have double quotes around it.
   */
  private void updateRegionOnMembers(String regionObjectName, String regionFullPath,
      Cluster.Region region) throws IOException {

    try {
      List<String> memberNamesTemp = region.getMemberName();
      ArrayList<String> memberNames = new ArrayList<>(memberNamesTemp);

      List<Cluster.RegionOnMember> regionOnMemberList = new ArrayList<>();
      List<Cluster.RegionOnMember> regionOnMemberListNew = new ArrayList<>();
      Cluster.RegionOnMember[] regionOnMemberNames = region.getRegionOnMembers();

      if ((regionOnMemberNames != null) && (regionOnMemberNames.length > 0)) {
        regionOnMemberList =
            new ArrayList<>(Arrays.asList(regionOnMemberNames));
      }
      logger.debug("updateRegionOnMembers : # regionOnMembers objects in region = {}",
          regionOnMemberList.size());
      for (Cluster.RegionOnMember anRom : regionOnMemberList) {

        for (String memberName : memberNames) {
          if (anRom.getMemberName().equals(memberName)) {
            // Add regionOnMember object in new list
            regionOnMemberListNew.add(anRom);

            logger.debug("updateRegionOnMembers : Processing existing Member name = {}",
                anRom.getMemberName());
            String objectNameROM =
                PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_REGION + regionObjectName
                    + PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_MEMBER + anRom.getMemberName();
            ObjectName regionOnMemberMBean = new ObjectName(objectNameROM);
            logger.debug("updateRegionOnMembers : Object name = {}",
                regionOnMemberMBean.getCanonicalName());

            AttributeList attributeList = mbs.getAttributes(regionOnMemberMBean,
                PulseConstants.REGION_ON_MEMBER_MBEAN_ATTRIBUTES);
            for (Object o : attributeList) {
              Attribute attribute = (Attribute) o;
              String name = attribute.getName();
              switch (name) {
                case PulseConstants.MBEAN_ATTRIBUTE_ENTRYSIZE:
                  anRom.setEntrySize(getLongAttribute(attribute.getValue(), attribute.getName()));
                  logger.debug("updateRegionOnMembers : anRom.getEntrySize() = {}",
                      anRom.getEntrySize());
                  break;
                case PulseConstants.MBEAN_ATTRIBUTE_ENTRYCOUNT:
                  anRom.setEntryCount(getLongAttribute(attribute.getValue(), attribute.getName()));
                  logger.debug("updateRegionOnMembers : anRom.getEntryCount() = {}",
                      anRom.getEntryCount());
                  break;
                case PulseConstants.MBEAN_ATTRIBUTE_PUTSRATE:
                  anRom.setPutsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
                  logger.debug("updateRegionOnMembers : anRom.getPutsRate() = {}",
                      anRom.getPutsRate());
                  break;
                case PulseConstants.MBEAN_ATTRIBUTE_GETSRATE:
                  anRom.setGetsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
                  logger.debug("updateRegionOnMembers : anRom.getGetsRate() = {}",
                      anRom.getGetsRate());
                  break;
                case PulseConstants.MBEAN_ATTRIBUTE_DISKREADSRATE:
                  anRom.setDiskGetsRate(
                      getDoubleAttribute(attribute.getValue(), attribute.getName()));
                  logger.debug("updateRegionOnMembers : anRom.getDiskGetsRate() = {}",
                      anRom.getDiskGetsRate());
                  break;
                case PulseConstants.MBEAN_ATTRIBUTE_DISKWRITESRATE:
                  anRom.setDiskPutsRate(
                      getDoubleAttribute(attribute.getValue(), attribute.getName()));
                  logger.debug("updateRegionOnMembers : anRom.getDiskPutsRate() = {}",
                      anRom.getDiskPutsRate());
                  break;
                case PulseConstants.MBEAN_ATTRIBUTE_LOCALMAXMEMORY:
                  anRom.setLocalMaxMemory(
                      getIntegerAttribute(attribute.getValue(), attribute.getName()));
                  logger.debug("updateRegionOnMembers : anRom.getLocalMaxMemory() = {}",
                      anRom.getLocalMaxMemory());
                  break;
              }
            }

            anRom.getGetsPerSecTrend().add(anRom.getGetsRate());
            anRom.getPutsPerSecTrend().add(anRom.getPutsRate());
            anRom.getDiskReadsPerSecTrend().add(anRom.getDiskGetsRate());
            anRom.getDiskWritesPerSecTrend().add(anRom.getDiskPutsRate());
            logger.debug(
                "updateRegionOnMembers : Existing member on region : getGetsRate() = {}, getPutsRate() = {}, getDiskGetsRate() = {}, getDiskPutsRate() = {}",
                anRom.getGetsPerSecTrend().size(), anRom.getPutsPerSecTrend().size(),
                anRom.getDiskReadsPerSecTrend().size(), anRom.getDiskWritesPerSecTrend().size());
            // remove existing member names from list so only new ones will remain
            memberNames.remove(anRom.getMemberName());

            break;
          }
        }
      }

      logger.debug(
          "updateRegionOnMembers : Loop over remaining member names and adding new member in region. Existing count = {}",
          regionOnMemberList.size());
      logger.debug("updateRegionOnMembers : Remaining new members in this region = {}",
          memberNames.size());
      // loop over the remaining regions members and add new members for this region
      for (String memberName : memberNames) {
        String objectNameROM = PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_REGION + regionObjectName
            + PulseConstants.OBJECT_NAME_REGION_ON_MEMBER_MEMBER + memberName;
        ObjectName regionOnMemberMBean = new ObjectName(objectNameROM);
        Cluster.RegionOnMember regionOnMember = new Cluster.RegionOnMember();
        regionOnMember.setMemberName(memberName);
        regionOnMember.setRegionFullPath(regionFullPath);
        AttributeList attributeList = mbs.getAttributes(regionOnMemberMBean,
            PulseConstants.REGION_ON_MEMBER_MBEAN_ATTRIBUTES);
        for (Object o : attributeList) {
          Attribute attribute = (Attribute) o;
          String name = attribute.getName();
          switch (name) {
            case PulseConstants.MBEAN_ATTRIBUTE_ENTRYSIZE:
              regionOnMember
                  .setEntrySize(getLongAttribute(attribute.getValue(), attribute.getName()));
              break;
            case PulseConstants.MBEAN_ATTRIBUTE_ENTRYCOUNT:
              regionOnMember
                  .setEntryCount(getLongAttribute(attribute.getValue(), attribute.getName()));
              break;
            case PulseConstants.MBEAN_ATTRIBUTE_PUTSRATE:
              regionOnMember
                  .setPutsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
              break;
            case PulseConstants.MBEAN_ATTRIBUTE_GETSRATE:
              regionOnMember
                  .setGetsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
              break;
            case PulseConstants.MBEAN_ATTRIBUTE_DISKREADSRATE:
              regionOnMember
                  .setDiskGetsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
              break;
            case PulseConstants.MBEAN_ATTRIBUTE_DISKWRITESRATE:
              regionOnMember
                  .setDiskPutsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
              break;
            case PulseConstants.MBEAN_ATTRIBUTE_LOCALMAXMEMORY:
              regionOnMember.setLocalMaxMemory(
                  getIntegerAttribute(attribute.getValue(), attribute.getName()));
              break;
          }
        }

        regionOnMember.getGetsPerSecTrend().add(regionOnMember.getGetsRate());
        regionOnMember.getPutsPerSecTrend().add(regionOnMember.getPutsRate());
        regionOnMember.getDiskReadsPerSecTrend().add(regionOnMember.getDiskGetsRate());
        regionOnMember.getDiskWritesPerSecTrend().add(regionOnMember.getDiskPutsRate());

        logger.debug(
            "updateRegionOnMembers : Adding New member on region : getGetsRate() = {}, getPutsRate() = {}, getDiskGetsRate() = {}, getDiskPutsRate() = {}",
            regionOnMember.getGetsRate(), regionOnMember.getPutsRate(),
            regionOnMember.getDiskGetsRate(), regionOnMember.getDiskPutsRate());
        regionOnMemberListNew.add(regionOnMember);
      }

      // set region on member
      region.setRegionOnMembers(regionOnMemberListNew);
      logger.debug("updateRegionOnMembers : Total regions on member in region {} after update = {}",
          region.getFullPath(), region.getRegionOnMembers().length);
    } catch (MalformedObjectNameException | InstanceNotFoundException | ReflectionException e) {
      logger.warn(e);
    }
  }

  /**
   * function used to get attribute values of Cluster Region and map them to cluster region vo
   *
   * @param mbeanName Cluster Region MBean
   */
  private void updateClusterRegion(ObjectName mbeanName) throws IOException {

    try {

      AttributeList attributeList =
          mbs.getAttributes(mbeanName, PulseConstants.REGION_MBEAN_ATTRIBUTES);

      // retrieve the full path of the region
      String regionObjectName = mbeanName.getKeyProperty("name");
      String regionFullPath = null;
      for (Object value : attributeList) {
        Attribute attribute = (Attribute) value;

        if (attribute.getName().equals(PulseConstants.MBEAN_ATTRIBUTE_FULLPATH)) {
          regionFullPath = getStringAttribute(attribute.getValue(), attribute.getName());
          break;
        }
      }

      Cluster.Region region = cluster.getClusterRegions().get(regionFullPath);

      if (null == region) {
        region = new Cluster.Region();
      }

      for (Object o : attributeList) {

        Attribute attribute = (Attribute) o;

        String name = attribute.getName();
        switch (name) {
          case PulseConstants.MBEAN_ATTRIBUTE_MEMBERS:
            String[] memName = (String[]) attribute.getValue();
            region.getMemberName().clear();
            for (String s : memName) {
              region.getMemberName().add(s);
            }
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_FULLPATH:
            region.setFullPath(getStringAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_DISKREADSRATE:
            region.setDiskReadsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_DISKWRITESRATE:
            region.setDiskWritesRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_EMPTYNODES:
            region.setEmptyNode(getIntegerAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_GETSRATE:
            region.setGetsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_LRUEVICTIONRATE:
            region
                .setLruEvictionRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_PUTSRATE:
            region.setPutsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_REGIONTYPE:
            region.setRegionType(getStringAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_ENTRYSIZE:
            region.setEntrySize(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_SYSTEMREGIONENTRYCOUNT:
            region.setSystemRegionEntryCount(
                getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_MEMBERCOUNT:
            region.setMemberCount(getIntegerAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_PERSISTENTENABLED:
            region.setPersistentEnabled(
                getBooleanAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_NAME:
            region.setName(getStringAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_GATEWAYENABLED:
            region.setWanEnabled(getBooleanAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_DISKUSAGE:
            region.setDiskUsage(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
        }
      }

      // add for each member
      updateRegionOnMembers(regionObjectName, regionFullPath, region);

      cluster.addClusterRegion(regionFullPath, region);
      cluster.getDeletedRegions().remove(region.getFullPath());
      // Memory Reads and writes
      region.getPutsPerSecTrend().add(region.getPutsRate());
      region.getGetsPerSecTrend().add(region.getGetsRate());
      // Disk Reads and Writes
      region.getDiskReadsPerSecTrend().add(region.getDiskReadsRate());
      region.getDiskWritesPerSecTrend().add(region.getDiskWritesRate());

    } catch (InstanceNotFoundException | ReflectionException infe) {
      logger.warn(infe);
    }
  }

  private static boolean isQuoted(String value) {
    final int len = value.length();
    return len >= 2 && value.charAt(0) == '"' && value.charAt(len - 1) == '"';
  }

  private void updateClusterStatement(ObjectName mbeanName) throws IOException {

    try {

      AttributeList attributeList =
          mbs.getAttributes(mbeanName, PulseConstants.STATEMENT_MBEAN_ATTRIBUTES);
      // retrieve the full path of the region
      String statementDefinition = mbeanName.getKeyProperty("name");

      if (isQuoted(statementDefinition)) {
        statementDefinition = ObjectName.unquote(statementDefinition);
      }

      Cluster.Statement statement = cluster.getClusterStatements().get(statementDefinition);

      if (null == statement) {
        statement = new Cluster.Statement();
        statement.setQueryDefinition(statementDefinition);
      }

      for (Object o : attributeList) {
        Attribute attribute = (Attribute) o;
        String name = attribute.getName();
        switch (name) {
          case PulseConstants.MBEAN_ATTRIBUTE_NUMTIMESCOMPILED:
            statement
                .setNumTimesCompiled(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_NUMEXECUTION:
            statement.setNumExecution(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_NUMEXECUTIONSINPROGRESS:
            statement.setNumExecutionsInProgress(
                getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_NUMTIMESGLOBALINDEXLOOKUP:
            statement.setNumTimesGlobalIndexLookup(
                getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_NUMROWSMODIFIED:
            statement
                .setNumRowsModified(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_PARSETIME:
            statement.setParseTime(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_BINDTIME:
            statement.setBindTime(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_OPTIMIZETIME:
            statement.setOptimizeTime(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_ROUTINGINFOTIME:
            statement
                .setRoutingInfoTime(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_GENERATETIME:
            statement.setGenerateTime(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_TOTALCOMPILATIONTIME:
            statement.setTotalCompilationTime(
                getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_EXECUTIONTIME:
            statement.setExecutionTime(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_PROJECTIONTIME:
            statement
                .setProjectionTime(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_TOTALEXECUTIONTIME:
            statement
                .setTotalExecutionTime(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_ROWSMODIFICATIONTIME:
            statement.setRowsModificationTime(
                getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_QNNUMROWSSEEN:
            statement.setqNNumRowsSeen(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_QNMSGSENDTIME:
            statement.setqNMsgSendTime(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_QNMSGSERTIME:
            statement.setqNMsgSerTime(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_QNRESPDESERTIME:
            statement
                .setqNRespDeSerTime(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
        }
      }

      cluster.addClusterStatement(statementDefinition, statement);
      // TODO : to store data for sparklines later
      /*
       * region.getPutsPerSecTrend().add(region.getPutsRate());
       * region.getGetsPerSecTrend().add(region.getGetsRate());
       */
    } catch (InstanceNotFoundException | ReflectionException infe) {
      logger.warn(infe);
    }
  }

  /**
   * function used to iterate through all member attributes and return the updated member
   */
  private Cluster.Member initializeMember(ObjectName mbeanName, Cluster.Member member)
      throws InstanceNotFoundException, ReflectionException, IOException, IntrospectionException {

    MBeanAttributeInfo[] mbeanAttributes = mbs.getMBeanInfo(mbeanName).getAttributes();

    AttributeList attributeList =
        mbs.getAttributes(mbeanName,
            Arrays.stream(mbeanAttributes).map(MBeanAttributeInfo::getName).distinct()
                .toArray(String[]::new));

    for (Object o : attributeList) {

      Attribute attribute = (Attribute) o;
      String name = attribute.getName();

      switch (name) {
        case PulseConstants.MBEAN_ATTRIBUTE_GEMFIREVERSION:
          if (member.getGemfireVersion() == null) {
            // Set Member's GemFire Version if not set already
            String gemfireVersion =
                obtainGemfireVersion(getStringAttribute(attribute.getValue(), attribute.getName()));
            member.setGemfireVersion(gemfireVersion);
          }
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_MANAGER:
          member.setManager(getBooleanAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_TOTALREGIONCOUNT:
          member
              .setTotalRegionCount(getIntegerAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_LOCATOR:
          member.setLocator(getBooleanAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_TOTALDISKUSAGE:
          member.setTotalDiskUsage(getLongAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_SERVER:
          member.setServer(getBooleanAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_TOTALFILEDESCRIPTOROPEN:
          member.setTotalFileDescriptorOpen(
              getLongAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_LOADAVERAGE:
          member.setLoadAverage(getDoubleAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_DISKWRITESRATE:
          member.setThroughputWrites(getDoubleAttribute(attribute.getValue(), attribute.getName()));
          member.getThroughputWritesTrend().add(member.getThroughputWrites());
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_DISKREADSRATE:
          member.setThroughputReads(getDoubleAttribute(attribute.getValue(), attribute.getName()));
          member.getThroughputReadsTrend().add(member.getThroughputReads());
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_JVMPAUSES:
          long trendVal = determineCurrentJVMPauses(PulseConstants.JVM_PAUSES_TYPE_MEMBER,
              member.getName(), getLongAttribute(attribute.getValue(), attribute.getName()));
          member.setGarbageCollectionCount(trendVal);
          member.getGarbageCollectionSamples().add(member.getGarbageCollectionCount());
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_USEDMEMORY:
          member.setCurrentHeapSize(getLongAttribute(attribute.getValue(), attribute.getName()));
          member.getHeapUsageSamples().add(member.getCurrentHeapSize());
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_MAXMEMORY:
          member.setMaxHeapSize(getLongAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_NUMTHREADS:
          member.setNumThreads(getIntegerAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_MEMBERUPTIME:
          member.setUptime(getLongAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_HOST:
          member.setHost(getStringAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_HOSTNAMEFORCLIENTS:
          member
              .setHostnameForClients(getStringAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_BINDADDRESS:
          member.setBindAddress(getStringAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_TOTALBYTESONDISK:
          member.setTotalBytesOnDisk(getLongAttribute(attribute.getValue(), attribute.getName()));
          member.getTotalBytesOnDiskSamples().add(member.getTotalBytesOnDisk());
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_CPUUSAGE:
          member.setCpuUsage(getDoubleAttribute(attribute.getValue(), attribute.getName()));
          member.getCpuUsageSamples().add(member.getCpuUsage());
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_HOSTCPUUSAGE:
          // Float value is expected for host cpu usage.
          // TODO Remove Float.valueOf() when float value is provided in mbean
          member.setHostCpuUsage(
              Double.valueOf(getIntegerAttribute(attribute.getValue(), attribute.getName())));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_MEMBER:
          member.setName(getStringAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_ID:
          member.setId(getStringAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_AVERAGEREADS:
          member.setGetsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
          member.getGetsPerSecond().add(member.getGetsRate());
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_AVERAGEWRITES:
          member.setPutsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
          member.getPutsPerSecond().add(member.getPutsRate());
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_OFFHEAPFREESIZE:
          member.setOffHeapFreeSize(getLongAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_OFFHEAPUSEDSIZE:
          member.setOffHeapUsedSize(getLongAttribute(attribute.getValue(), attribute.getName()));
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_SERVERGROUPS:
          String[] sgValues = (String[]) attribute.getValue();
          member.getServerGroups().clear();
          for (String sgValue : sgValues) {
            member.getServerGroups().add(sgValue);
          }
          break;
        case PulseConstants.MBEAN_ATTRIBUTE_REDUNDANCYZONES:
          String rzValue = "";
          if (null != attribute.getValue()) {
            rzValue = getStringAttribute(attribute.getValue(), attribute.getName());
          }
          member.getRedundancyZones().clear();
          if (!rzValue.isEmpty()) {
            member.getRedundancyZones().add(rzValue);
          }
          break;
      }
    }

    return member;
  }

  /**
   * function used to get attribute values of Cluster Member and map them to cluster member vo
   *
   * @param mbeanName Cluster Member MBean
   */
  private void updateClusterMember(ObjectName mbeanName) throws IOException {

    try {
      String memberName = mbeanName.getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

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
      ArrayList<Cluster.Member> memberArrList =
          (ArrayList<Cluster.Member>) cluster.getPhysicalToMember().get(clusterMember.getHost());
      if (memberArrList != null) {
        if (!memberArrList.contains(clusterMember)) {
          memberArrList.add(clusterMember);
        }
      } else {
        ArrayList<Cluster.Member> memberList = new ArrayList<>();
        memberList.add(clusterMember);
        cluster.getPhysicalToMember().put(clusterMember.getHost(), memberList);
      }
    } catch (InstanceNotFoundException | ReflectionException | IntrospectionException infe) {
      logger.warn(infe);
    }
  }

  /**
   * function used to handle Integer data type if the value for mbean for an attribute is null then
   * return 0 as default value else return the attribute value
   */
  private Integer getIntegerAttribute(Object object, String name) {
    if (object == null) {
      return 0;
    }

    try {
      if (!(object.getClass().equals(Integer.class))) {
        logger.info(
            "************************Unexpected type for attribute: {}; Expected type: {}; Received type: {}************************",
            name, Integer.class.getName(), object.getClass().getName());
        return 0;
      } else {
        return (Integer) object;
      }
    } catch (Exception e) {
      logger.info("Exception occurred: ", e);
      return 0;
    }
  }

  /**
   * function used to handle Long data type if the value for mbean for an attribute is null then
   * return 0 as default value else return the attribute value
   */
  private Long getLongAttribute(Object object, String name) {
    if (object == null) {
      return 0L;
    }

    try {
      if (!(object.getClass().equals(Long.class))) {
        logger.info(
            "************************Unexpected type for attribute: {}; Expected type: {}; Received type: {}************************",
            name, Long.class.getName(), object.getClass().getName());
        return 0L;
      } else {
        return (Long) object;
      }
    } catch (Exception e) {
      logger.info("Exception occurred: ", e);
      return 0L;
    }

  }

  /**
   * function used to handle String data type if the value for mbean for an attribute is null then
   * return the empty string as default value else return the attribute value
   */
  private String getStringAttribute(Object object, String name) {
    if (object == null) {
      return "";
    }

    try {
      if (!(object.getClass().equals(String.class))) {
        logger.info(
            "************************Unexpected type for attribute: {}; Expected type: {}; Received type: {}************************",
            name, String.class.getName(), object.getClass().getName());
        return "";
      } else {
        return (String) object;
      }
    } catch (Exception e) {
      logger.info("Exception occurred: ", e);
      return "";
    }
  }

  /**
   * function used to handle Boolean data type if the value for mbean for an attribute is null then
   * return false as default value else return the attribute value
   */
  private Boolean getBooleanAttribute(Object object, String name) {
    if (object == null) {
      return Boolean.FALSE;
    }

    try {
      if (!(object.getClass().equals(Boolean.class))) {
        logger.info(
            "************************Unexpected type for attribute: {}; Expected type: {}; Received type: {}************************",
            name, Boolean.class.getName(), object.getClass().getName());
        return Boolean.FALSE;
      } else {
        return (Boolean) object;
      }
    } catch (Exception e) {
      logger.info("Exception Occurred: ", e);
      return Boolean.FALSE;
    }
  }

  /**
   * function used to handle Double data type if the value for mbean for an attribute is null then
   * return 0.0 as default value else return the attribute value
   */
  Double getDoubleAttribute(Object object, String name) {
    if (object == null) {
      return (double) 0;
    }

    try {
      if (object instanceof Float) {
        return BigDecimal.valueOf((Float) object).doubleValue();
      } else if (object instanceof Double) {
        return (Double) object;
      } else {
        logger.info(
            "************************Unexpected type for attribute: {}; Expected type: {}; Received type: {}************************",
            name, Double.class.getName(), object.getClass().getName());
        return (double) 0;
      }
    } catch (Exception e) {
      logger.info("Exception occurred: ", e);
      return (double) 0;
    }
  }

  /**
   * function used to get attribute values of Member Region and map them to Member vo
   *
   * @param mbeanName Member Region MBean
   */
  private void updateMemberRegion(ObjectName mbeanName) throws IOException {

    try {
      String memberName = mbeanName.getKeyProperty(PulseConstants.MBEAN_KEY_PROPERTY_MEMBER);

      Cluster.Member member = cluster.getMembersHMap().get(memberName);

      // Following attributes are not present in 9.0
      // "Members"
      // "EmptyNodes"
      // "SystemRegionEntryCount"
      // "MemberCount"
      AttributeList attributeList =
          mbs.getAttributes(mbeanName, PulseConstants.REGION_MBEAN_ATTRIBUTES);

      // retrieve the full path of the region
      String regionFullPathKey = null;
      for (Object value : attributeList) {
        Attribute attribute = (Attribute) value;

        if (attribute.getName().equals(PulseConstants.MBEAN_ATTRIBUTE_FULLPATH)) {
          regionFullPathKey = getStringAttribute(attribute.getValue(), attribute.getName());
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
      for (Object o : attributeList) {
        Attribute attribute = (Attribute) o;
        String name = attribute.getName();
        switch (name) {
          case PulseConstants.MBEAN_ATTRIBUTE_FULLPATH:
            region.setFullPath(getStringAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_DISKREADSRATE:
            region.setDiskReadsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_DISKWRITESRATE:
            region.setDiskWritesRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_GETSRATE:
            region.setGetsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_LRUEVICTIONRATE:
            region
                .setLruEvictionRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_PUTSRATE:
            region.setPutsRate(getDoubleAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_REGIONTYPE:
            region.setRegionType(getStringAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_MEMBERCOUNT:
            region.setMemberCount(getIntegerAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_ENTRYSIZE:
            region.setEntrySize(getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_ENTRYCOUNT:
            region.setSystemRegionEntryCount(
                getLongAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_NAME:
            region.setName(getStringAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_PERSISTENTENABLED:
            region.setPersistentEnabled(
                getBooleanAttribute(attribute.getValue(), attribute.getName()));
            break;
          case PulseConstants.MBEAN_ATTRIBUTE_GATEWAYENABLED:
            region.setWanEnabled(getBooleanAttribute(attribute.getValue(), attribute.getName()));
            break;
        }
      }
      /*
       * GemfireXD related code try{// Added for Rolling upgrade changes. Needs to removed once
       * Rolling upgrade handled gracefully CompositeData compositeData = (CompositeData)
       * (this.mbs.invoke(mbeanName, PulseConstants.MBEAN_OPERATION_LISTREGIONATTRIBUTES, null,
       * null));
       *
       * if (compositeData != null) { if
       * (compositeData.containsKey(PulseConstants.COMPOSITE_DATA_KEY_SCOPE)) {
       * region.setScope((String) compositeData .get(PulseConstants.COMPOSITE_DATA_KEY_SCOPE)); }
       * else if (compositeData .containsKey(PulseConstants.COMPOSITE_DATA_KEY_DISKSTORENAME)) {
       * region.setDiskStoreName((String) compositeData
       * .get(PulseConstants.COMPOSITE_DATA_KEY_DISKSTORENAME)); } else if (compositeData
       * .containsKey(PulseConstants.COMPOSITE_DATA_KEY_DISKSYNCHRONOUS)) {
       * region.setDiskSynchronous((Boolean) compositeData
       * .get(PulseConstants.COMPOSITE_DATA_KEY_DISKSYNCHRONOUS)); } } }catch (MBeanException anfe)
       * { logger.warn(anfe); }catch (javax.management.RuntimeMBeanException anfe) {
       * region.setScope(""); region.setDiskStoreName(""); region.setDiskSynchronous(false);
       * //logger.
       * warning("Some of the Pulse elements are not available currently. There might be a GemFire upgrade going on."
       * ); }
       *
       *
       * // Remove deleted regions from member's regions list for (Iterator<String> it =
       * cluster.getDeletedRegions().iterator(); it .hasNext();) { String deletedRegion = it.next();
       * if (member.getMemberRegions().get(deletedRegion) != null) {
       * member.getMemberRegions().remove(deletedRegion); }
       * member.setTotalRegionCount(member.getMemberRegions().size()); }
       */
    } catch (InstanceNotFoundException | ReflectionException infe) {
      logger.warn(infe);
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
      long timeStamp = notification.getTimeStamp();
      Date date = new Date(timeStamp);
      alert.setTimestamp(date);
      String notificationSource = (String) notification.getUserData();
      alert.setMemberName(notificationSource);
      String alertDescription = notification.getMessage();
      if (alertDescription.startsWith("[error")) {
        alert.setSeverity(Cluster.Alert.ERROR);
      } else if (alertDescription.startsWith("[warning")) {
        alert.setSeverity(Cluster.Alert.WARNING);
      } else if (alertDescription.startsWith("[severe")) {
        alert.setSeverity(Cluster.Alert.SEVERE);
      } else {
        alert.setSeverity(Cluster.Alert.INFO);
      }
      alert.setDescription(notification.getMessage());
      alert.setAcknowledged(false);
      alert.setId(Cluster.Alert.nextID());
      cluster.addAlert(alert);
    } else {
      Cluster.Alert alert = new Cluster.Alert();
      long timeStamp = notification.getTimeStamp();
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

      if (PulseConstants.NOTIFICATION_TYPE_REGION_DESTROYED.equals(type)) {
        // Remove deleted region from member's regions list
        String msg = notification.getMessage();
        String deletedRegion = msg.substring(msg.indexOf("Name ") + "Name ".length());
        Cluster.Member member = cluster.getMembersHMap().get(notificationSource);

        if (member.getMemberRegions().get(deletedRegion) != null) {
          member.getMemberRegions().remove(deletedRegion);
          member.setTotalRegionCount(member.getMemberRegions().size());
        }
      }
    }
  }

  @Override
  public ObjectNode executeQuery(String queryText, String members, int limit) {

    ObjectNode queryResult = mapper.createObjectNode();

    if (mbs != null && systemMBeans != null) {
      Object[] opParams = {queryText, members, limit};
      for (ObjectName sysMBean : systemMBeans) {
        try {
          String resultString = (String) (mbs.invoke(sysMBean,
              PulseConstants.MBEAN_OPERATION_QUERYDATABROWSER, opParams, opSignature));

          // Convert result into JSON
          queryResult = (ObjectNode) mapper.readTree(resultString);

        } catch (Exception e) {
          // Send error into result
          queryResult.put("error", e.getMessage());
          logger.debug(e);
        }
      }
    }

    return queryResult;
  }

  // Function to find out number of current JVM pauses on cluster or Member
  private long determineCurrentJVMPauses(String type, String key, long totalJVMPauses) {

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
    int startIndex = version.indexOf(versionText) + versionText.length();
    String gemfireVersion = version.substring(startIndex);
    gemfireVersion = gemfireVersion.substring(0, gemfireVersion.indexOf(" "));

    return gemfireVersion;
  }
}
