/*=========================================================================
 *Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *All Rights Reserved.
 *========================================================================
 */
package com.gemstone.gemfire.admin.jmx.internal;

//import com.gemstone.gemfire.admin.AdminException;
//import com.gemstone.gemfire.admin.DistributionLocator;
import com.gemstone.gemfire.admin.DistributionLocatorConfig;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
//import com.gemstone.gemfire.internal.Assert;

//import org.apache.commons.modeler.ManagedBean;
//import org.apache.commons.modeler.AttributeInfo;

//import java.util.Date;
//import java.util.Set;

//import javax.management.Attribute;
//import javax.management.AttributeList;
//import javax.management.Descriptor;
//import javax.management.JMException;
//import javax.management.MBeanServer;
//import javax.management.MalformedObjectNameException;
//import javax.management.Notification;
//import javax.management.NotificationListener;
import javax.management.ObjectName;
//import javax.management.modelmbean.DescriptorSupport;
import javax.management.modelmbean.ModelMBean;
//import javax.management.modelmbean.ModelMBeanAttributeInfo;

/**
 * Provides MBean support for managing a distribution locator.
 *
 * @author    Kirk Lund
 */
public class DistributionLocatorJmxImpl 
extends com.gemstone.gemfire.admin.internal.DistributionLocatorImpl
implements com.gemstone.gemfire.admin.jmx.internal.ManagedResource,
           DistributionLocatorConfig {

  /** The JMX object name of this managed resource */
  private ObjectName objectName;

  // -------------------------------------------------------------------------
  //   Constructor(s)
  // -------------------------------------------------------------------------
  
  /**
   * Constructs new instance of DistributionLocatorJmxImpl for managing a
   * distribution locator service via JMX.
   */
  public DistributionLocatorJmxImpl(DistributionLocatorConfig config,
                                    AdminDistributedSystemImpl system) {
    super(config, system);
    initializeMBean();
  }

  /** Create and register the MBean to manage this resource */
  private void initializeMBean() {
    this.mbeanName = "GemFire:type=DistributionLocator,id=" + MBeanUtil.makeCompliantMBeanNameProperty(getId());
    this.objectName =
        MBeanUtil.createMBean(this, MBeanUtil.lookupManagedBean(this));
  }

  ////////////////////////  Configuration  ////////////////////////

  public String getHost() {
    return this.getConfig().getHost();
  }

  public void setHost(String host) {
    this.getConfig().setHost(host);
  }

  public String getWorkingDirectory() {
    return this.getConfig().getWorkingDirectory();
  }

  public void setWorkingDirectory(String dir) {
    this.getConfig().setWorkingDirectory(dir);
  }

  public String getProductDirectory() {
    return this.getConfig().getProductDirectory();
  }

  public void setProductDirectory(String dir) {
    this.getConfig().setProductDirectory(dir);
  }

  public String getRemoteCommand() {
    return this.getConfig().getRemoteCommand();
  }

  public void setRemoteCommand(String remoteCommand) {
    this.getConfig().setRemoteCommand(remoteCommand);
  }

  public java.util.Properties getDistributedSystemProperties() {
    return this.getConfig().getDistributedSystemProperties();
  }
  
  public void setDistributedSystemProperties(java.util.Properties props) {
    this.getConfig().setDistributedSystemProperties(props);
  }

  public void validate() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public int getPort() {
    return this.getConfig().getPort();
  }

  public void setPort(int port) {
    this.getConfig().setPort(port);
  }

  public String getBindAddress() {
    return this.getConfig().getBindAddress();
  }

  public void setBindAddress(String bindAddress) {
    this.getConfig().setBindAddress(bindAddress);
  }

  // -------------------------------------------------------------------------
  //   MBean attributes - accessors/mutators
  // -------------------------------------------------------------------------
  
  // -------------------------------------------------------------------------
  //   JMX Notification listener
  // -------------------------------------------------------------------------

  // -------------------------------------------------------------------------
  //   ManagedResource implementation
  // -------------------------------------------------------------------------
  
  /** The name of the MBean that will manage this resource */
  private String mbeanName;

  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;
  
	public String getMBeanName() {
		return this.mbeanName;
	}
  
	public ModelMBean getModelMBean() {
		return this.modelMBean;
	}
	public void setModelMBean(ModelMBean modelMBean) {
		this.modelMBean = modelMBean;
	}
  
  public ObjectName getObjectName() {
    return this.objectName;
  }

  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.DISTRIBUTION_LOCATOR;
  }
  
  public void cleanupResource() {}
  
}

