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
package org.apache.geode.management.internal.cli.shell;

import static org.apache.geode.internal.net.SSLConfigurationFactory.GEODE_SSL_CONFIG_PROPERTIES;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnectionNotification;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.healthmarketscience.rmiio.RemoteOutputStreamClient;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.net.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.internal.ContextAwareSSLRMIClientSocketFactory;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.beans.FileUploader;
import org.apache.geode.management.internal.beans.FileUploaderMBean;
import org.apache.geode.management.internal.cli.CommandRequest;
import org.apache.geode.management.internal.security.ResourceConstants;

/**
 * OperationInvoker JMX Implementation
 *
 * @since GemFire 7.0
 */
public class JmxOperationInvoker implements OperationInvoker {
  private static final Logger logger = LogService.getLogger();

  private static final String JMX_URL_FORMAT = "service:jmx:rmi://{0}/jndi/rmi://{0}:{1}/jmxrmi";

  // an JMX object describing the client-end of a JMX connection
  private JMXConnector connector;

  // address of the JMX Connector Server
  private JMXServiceURL url;

  // an instance of an MBeanServer connection (in a connected state)
  private MBeanServerConnection mbsc;

  // String representation of the GemFire JMX Manager endpoint, including host and port
  private String endpoints;

  // MBean Proxies
  private DistributedSystemMXBean distributedSystemMXBeanProxy;
  private MemberMXBean memberMXBeanProxy;
  private FileUploaderMBean fileUploadMBeanProxy;

  private ObjectName managerMemberObjectName;

  final AtomicBoolean isConnected = new AtomicBoolean(false);
  final AtomicBoolean isSelfDisconnect = new AtomicBoolean(false);

  private int clusterId = CLUSTER_ID_WHEN_NOT_CONNECTED;

  public JmxOperationInvoker(final String host, final int port, Properties gfProperties)
      throws Exception {
    this.endpoints = host + "[" + port + "]"; // Use the same syntax as the "connect" command.

    // Modify check period from default (60 sec) to 1 sec
    final Map<String, Object> env = new HashMap<>();

    env.put(JMXConnectionListener.CHECK_PERIOD_PROP, JMXConnectionListener.CHECK_PERIOD);

    // when not using JMXShiroAuthenticator in the integrated security, JMX own password file
    // authentication requires the credentials been sent in String[] format.
    // Our JMXShiroAuthenticator handles both String[] and String format.
    String token = gfProperties.getProperty(ResourceConstants.TOKEN);
    String username = gfProperties.getProperty(ResourceConstants.USER_NAME);
    String password = gfProperties.getProperty(ResourceConstants.PASSWORD);
    if (token != null) {
      env.put(JMXConnector.CREDENTIALS, token);
    } else if (username != null) {
      env.put(JMXConnector.CREDENTIALS, new String[] {username, password});
    }

    SSLConfig sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(gfProperties,
        SecurableCommunicationChannel.JMX);

    if (sslConfig.isEnabled()) {
      StringWriter propertiesWriter = new StringWriter();
      gfProperties.store(propertiesWriter, null);
      System.setProperty(GEODE_SSL_CONFIG_PROPERTIES, propertiesWriter.toString());
      env.put("com.sun.jndi.rmi.factory.socket", new ContextAwareSSLRMIClientSocketFactory());
    }

    final String hostName = checkAndConvertToCompatibleIPv6Syntax(host);
    this.url = new JMXServiceURL(MessageFormat.format(JMX_URL_FORMAT,
        hostName, String.valueOf(port)));
    this.connector = JMXConnectorFactory.connect(url, env);
    this.mbsc = connector.getMBeanServerConnection();
    this.connector.addConnectionNotificationListener(new JMXConnectionListener(this), null, null);
    this.distributedSystemMXBeanProxy = JMX.newMXBeanProxy(mbsc,
        MBeanJMXAdapter.getDistributedSystemName(), DistributedSystemMXBean.class);

    if (this.distributedSystemMXBeanProxy == null) {
      logger.info(
          "DistributedSystemMXBean is not present on member with endpoints : " + this.endpoints);
      throw new JMXConnectionException(JMXConnectionException.MANAGER_NOT_FOUND_EXCEPTION);
    } else {
      this.managerMemberObjectName = this.distributedSystemMXBeanProxy.getMemberObjectName();
      if (this.managerMemberObjectName == null || !JMX.isMXBeanInterface(MemberMXBean.class)) {
        logger.info("MemberMXBean with ObjectName " + this.managerMemberObjectName
            + " is not present on member with endpoints : " + endpoints);
        throw new JMXConnectionException(JMXConnectionException.MANAGER_NOT_FOUND_EXCEPTION);
      } else {
        this.memberMXBeanProxy =
            JMX.newMXBeanProxy(mbsc, managerMemberObjectName, MemberMXBean.class);
        this.fileUploadMBeanProxy = JMX.newMBeanProxy(mbsc,
            new ObjectName(ManagementConstants.OBJECTNAME__FILEUPLOADER_MBEAN),
            FileUploaderMBean.class);
      }
    }

    this.isConnected.set(true);
    this.clusterId = distributedSystemMXBeanProxy.getDistributedSystemId();
  }

  @Override
  public Object getAttribute(String resourceName, String attributeName)
      throws JMXInvocationException {
    try {
      return mbsc.getAttribute(ObjectName.getInstance(resourceName), attributeName);
    } catch (AttributeNotFoundException e) {
      throw new JMXInvocationException(attributeName + " not found for " + resourceName, e);
    } catch (InstanceNotFoundException e) {
      throw new JMXInvocationException(resourceName + " is not registered in the MBean server.", e);
    } catch (MalformedObjectNameException | IOException e) {
      throw new JMXInvocationException(resourceName + " is not a valid resource name.", e);
    } catch (MBeanException e) {
      throw new JMXInvocationException(
          "Exception while fetching " + attributeName + " for " + resourceName, e);
    } catch (ReflectionException e) {
      throw new JMXInvocationException("Couldn't find " + attributeName + " for " + resourceName,
          e);
    } catch (NullPointerException e) {
      throw new JMXInvocationException("Given resourceName is null.", e);
    }
  }

  @Override
  public String getRemoteVersion() {
    return memberMXBeanProxy.getReleaseVersion();
  }

  @Override
  public String getRemoteGeodeSerializationVersion() {
    return memberMXBeanProxy.getGeodeReleaseVersion();
  }

  @Override
  public Object invoke(String resourceName, String operationName, Object[] params,
      String[] signature) throws JMXInvocationException {
    try {
      return invoke(ObjectName.getInstance(resourceName), operationName, params, signature);
    } catch (MalformedObjectNameException e) {
      throw new JMXInvocationException(resourceName + " is not a valid resource name.", e);
    } catch (NullPointerException e) {
      throw new JMXInvocationException("Given resourceName is null.", e);
    }
  }

  /**
   * JMX Specific operation invoke caller.
   *
   *
   * @return result of JMX Operation invocation
   *
   */
  protected Object invoke(ObjectName resource, String operationName, Object[] params,
      String[] signature) throws JMXInvocationException {
    try {
      return mbsc.invoke(resource, operationName, params, signature);
    } catch (InstanceNotFoundException e) {
      throw new JMXInvocationException(resource + " is not registered in the MBean server.", e);
    } catch (MBeanException e) {
      throw new JMXInvocationException(
          "Exception while invoking " + operationName + " on " + resource, e);
    } catch (ReflectionException e) {
      throw new JMXInvocationException("Couldn't find " + operationName + " on " + resource
          + " with arguments " + Arrays.toString(signature), e);
    } catch (IOException e) {
      throw new JMXInvocationException("Couldn't communicate with remote server at " + toString(),
          e);
    }
  }

  @Override
  public Set<ObjectName> queryNames(final ObjectName objectName, final QueryExp queryExpression) {
    try {
      return getMBeanServerConnection().queryNames(objectName, queryExpression);
    } catch (IOException e) {
      throw new JMXInvocationException(String
          .format("Failed to communicate with the remote MBean server at (%1$s)!", toString()), e);
    }
  }

  @Override
  // this should only returns a json representation of ResultModel
  public String processCommand(final CommandRequest commandRequest) {
    List<String> stagedFilePaths = null;

    try {
      if (commandRequest.hasFileList()) {
        stagedFilePaths = new ArrayList<>();
        for (File file : commandRequest.getFileList()) {
          FileUploader.RemoteFile remote = fileUploadMBeanProxy.uploadFile(file.getName());
          FileInputStream source = new FileInputStream(file);

          OutputStream target = RemoteOutputStreamClient.wrap(remote.getOutputStream());
          IOUtils.copyLarge(source, target);
          target.close();

          stagedFilePaths.add(remote.getFilename());
        }
      }
    } catch (IOException e) {
      throw new JMXInvocationException("Unable to upload file", e);
    }

    try {
      return memberMXBeanProxy.processCommand(commandRequest.getUserInput(),
          commandRequest.getEnvironment(), stagedFilePaths);
    } finally {
      if (stagedFilePaths != null) {
        fileUploadMBeanProxy.deleteFiles(stagedFilePaths);
      }
    }
  }

  @Override
  public void stop() {
    try {
      this.isSelfDisconnect.set(true);
      this.connector.close();
      this.isConnected.set(false);
    } catch (IOException e) {
      // ignore exceptions occurring while closing the connector
    }
  }

  @Override
  public boolean isConnected() {
    return this.isConnected.get();
  }

  @Override
  public DistributedSystemMXBean getDistributedSystemMXBean() {
    if (distributedSystemMXBeanProxy == null) {
      throw new IllegalStateException(
          "The DistributedSystemMXBean proxy was not initialized properly!");
    }
    return distributedSystemMXBeanProxy;
  }

  public JMXServiceURL getJmxServiceUrl() {
    return this.url;
  }

  @Override
  public <T> T getMBeanProxy(final ObjectName objectName, final Class<T> mbeanInterface) {
    if (DistributedSystemMXBean.class.equals(mbeanInterface)
        && ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN.equals(objectName.toString())) {
      return mbeanInterface.cast(getDistributedSystemMXBean());
    } else if (JMX.isMXBeanInterface(mbeanInterface)) {
      return JMX.newMXBeanProxy(getMBeanServerConnection(), objectName, mbeanInterface);
    } else {
      return JMX.newMBeanProxy(getMBeanServerConnection(), objectName, mbeanInterface);
    }
  }

  public MBeanServerConnection getMBeanServerConnection() {
    if (this.mbsc == null) {
      throw new IllegalStateException("Gfsh is not connected to the GemFire Manager.");
    }
    return this.mbsc;
  }

  @Override
  public boolean isReady() {
    try {
      return this.mbsc.isRegistered(managerMemberObjectName);
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public String toString() {
    return this.endpoints;
  }

  @Override
  public int getClusterId() {
    return this.clusterId;
  }

  void resetClusterId() {
    clusterId = CLUSTER_ID_WHEN_NOT_CONNECTED;
  }

  /**
   * If the given host address contains a ":", considers it as an IPv6 address & returns the host
   * based on RFC2732 requirements i.e. surrounds the given host address string with square
   * brackets. If ":" is not found in the given string, simply returns the same string.
   *
   * @param hostAddress host address to check if it's an IPv6 address
   *
   * @return for an IPv6 address returns compatible host address otherwise returns the same string
   */
  private static String checkAndConvertToCompatibleIPv6Syntax(String hostAddress) {
    // if host string contains ":", considering it as an IPv6 Address
    // Conforming to RFC2732 - http://www.ietf.org/rfc/rfc2732.txt
    if (hostAddress.contains(":")) {
      logger.debug("IPv6 host address detected, using IPv6 syntax for host in JMX connection URL");
      hostAddress = "[" + hostAddress + "]";
      logger.debug("Compatible host address is : " + hostAddress);
    }
    return hostAddress;
  }
}


/**
 * A Connection Notification Listener. Notifies Gfsh when a connection gets terminated abruptly.
 *
 * @since GemFire 7.0
 */
class JMXConnectionListener implements NotificationListener {
  static final String CHECK_PERIOD_PROP = "jmx.remote.x.client.connection.check.period";
  static final long CHECK_PERIOD = 1000L;
  private JmxOperationInvoker invoker;

  JMXConnectionListener(JmxOperationInvoker invoker) {
    this.invoker = invoker;
  }

  @Override
  public void handleNotification(Notification notification, Object handback) {
    if (notification instanceof JMXConnectionNotification) {
      JMXConnectionNotification connNotif = (JMXConnectionNotification) notification;
      if (JMXConnectionNotification.CLOSED.equals(connNotif.getType())
          || JMXConnectionNotification.FAILED.equals(connNotif.getType())) {
        this.invoker.isConnected.set(false);
        this.invoker.resetClusterId();
        if (!this.invoker.isSelfDisconnect.get()) {
          Gfsh.getCurrentInstance().notifyDisconnect(this.invoker.toString());
        }
      }
    }
  }
}
