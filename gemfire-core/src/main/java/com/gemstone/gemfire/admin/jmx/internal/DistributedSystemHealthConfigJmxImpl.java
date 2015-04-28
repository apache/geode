/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.admin.jmx.internal;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.admin.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import javax.management.*;
import javax.management.modelmbean.*;
//import org.apache.commons.modeler.ManagedBean;

/**
 * The JMX "managed resource" that represents the configuration for
 * the health of a distributed system.  Basically, it provides the
 * behavior of <code>DistributedSystemHealthConfigImpl</code>, but
 * does some JMX stuff like registering beans with the agent.
 *
 * @see GemFireHealthJmxImpl#createDistributedSystemHealthConfig
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public class DistributedSystemHealthConfigJmxImpl
  extends DistributedSystemHealthConfigImpl 
  implements ManagedResource {

  /** The <code>GemFireHealth</code> that we help configure */
  private GemFireHealth health;

  /** The name of the MBean that will manage this resource */
  private String mbeanName;

  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;

  /** The JMX object name of the MBean for this managed resource */
  private final ObjectName objectName;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>DistributedSystemHealthCOnfigJmxImpl</code>
   * that configures the health of the distributed system monitored by
   * <code>health</code>.
   */
  DistributedSystemHealthConfigJmxImpl(GemFireHealthJmxImpl health)
    throws AdminException {

    super();
    this.health = health;
    this.mbeanName = new StringBuffer()
      .append(MBEAN_NAME_PREFIX)
      .append("DistributedSystemHealthConfig,id=")
      .append(MBeanUtil.makeCompliantMBeanNameProperty(health.getDistributedSystem().getId()))
      .toString();
    this.objectName = MBeanUtil.createMBean(this);
  }

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Applies the changes made to this config back to the health
   * monitor.
   *
   * @see GemFireHealth#setDistributedSystemHealthConfig
   */
  public void applyChanges() {
    this.health.setDistributedSystemHealthConfig(this);
  }

  public String getMBeanName() {
    return this.mbeanName;
  }
  
  public ModelMBean getModelMBean() {
    return this.modelMBean;
  }

  public void setModelMBean(ModelMBean modelMBean) {
    this.modelMBean = modelMBean;
  }

  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.DISTRIBUTED_SYSTEM_HEALTH_CONFIG;
  }

  public ObjectName getObjectName() {
    return this.objectName;
  }

  public void cleanupResource() {}

}
