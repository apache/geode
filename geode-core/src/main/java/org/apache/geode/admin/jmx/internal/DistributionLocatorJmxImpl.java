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
package org.apache.geode.admin.jmx.internal;

import javax.management.ObjectName;
import javax.management.modelmbean.ModelMBean;

import org.apache.geode.admin.DistributionLocatorConfig;
import org.apache.geode.admin.internal.AdminDistributedSystemImpl;

/**
 * Provides MBean support for managing a distribution locator.
 *
 */
public class DistributionLocatorJmxImpl
    extends org.apache.geode.admin.internal.DistributionLocatorImpl
    implements org.apache.geode.admin.jmx.internal.ManagedResource, DistributionLocatorConfig {

  /** The JMX object name of this managed resource */
  private ObjectName objectName;

  // -------------------------------------------------------------------------
  // Constructor(s)
  // -------------------------------------------------------------------------

  /**
   * Constructs new instance of DistributionLocatorJmxImpl for managing a distribution locator
   * service via JMX.
   */
  public DistributionLocatorJmxImpl(DistributionLocatorConfig config,
      AdminDistributedSystemImpl system) {
    super(config, system);
    initializeMBean();
  }

  /** Create and register the MBean to manage this resource */
  private void initializeMBean() {
    mbeanName =
        "GemFire:type=DistributionLocator,id=" + MBeanUtils.makeCompliantMBeanNameProperty(getId());
    objectName = MBeanUtils.createMBean(this, MBeanUtils.lookupManagedBean(this));
  }

  //////////////////////// Configuration ////////////////////////

  @Override
  public String getHost() {
    return getConfig().getHost();
  }

  @Override
  public void setHost(String host) {
    getConfig().setHost(host);
  }

  @Override
  public String getWorkingDirectory() {
    return getConfig().getWorkingDirectory();
  }

  @Override
  public void setWorkingDirectory(String dir) {
    getConfig().setWorkingDirectory(dir);
  }

  @Override
  public String getProductDirectory() {
    return getConfig().getProductDirectory();
  }

  @Override
  public void setProductDirectory(String dir) {
    getConfig().setProductDirectory(dir);
  }

  @Override
  public String getRemoteCommand() {
    return getConfig().getRemoteCommand();
  }

  @Override
  public void setRemoteCommand(String remoteCommand) {
    getConfig().setRemoteCommand(remoteCommand);
  }

  @Override
  public java.util.Properties getDistributedSystemProperties() {
    return getConfig().getDistributedSystemProperties();
  }

  @Override
  public void setDistributedSystemProperties(java.util.Properties props) {
    getConfig().setDistributedSystemProperties(props);
  }

  @Override
  public void validate() {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    throw new UnsupportedOperationException("Should not be invoked");
  }

  @Override
  public int getPort() {
    return getConfig().getPort();
  }

  @Override
  public void setPort(int port) {
    getConfig().setPort(port);
  }

  @Override
  public String getBindAddress() {
    return getConfig().getBindAddress();
  }

  @Override
  public void setBindAddress(String bindAddress) {
    getConfig().setBindAddress(bindAddress);
  }

  // -------------------------------------------------------------------------
  // MBean attributes - accessors/mutators
  // -------------------------------------------------------------------------

  // -------------------------------------------------------------------------
  // JMX Notification listener
  // -------------------------------------------------------------------------

  // -------------------------------------------------------------------------
  // ManagedResource implementation
  // -------------------------------------------------------------------------

  /** The name of the MBean that will manage this resource */
  private String mbeanName;

  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;

  @Override
  public String getMBeanName() {
    return mbeanName;
  }

  @Override
  public ModelMBean getModelMBean() {
    return modelMBean;
  }

  @Override
  public void setModelMBean(ModelMBean modelMBean) {
    this.modelMBean = modelMBean;
  }

  @Override
  public ObjectName getObjectName() {
    return objectName;
  }

  @Override
  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.DISTRIBUTION_LOCATOR;
  }

  @Override
  public void cleanupResource() {}

}
