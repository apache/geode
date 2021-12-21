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

import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.admin.internal.DistributedSystemHealthConfigImpl;

/**
 * The JMX "managed resource" that represents the configuration for the health of a distributed
 * system. Basically, it provides the behavior of <code>DistributedSystemHealthConfigImpl</code>,
 * but does some JMX stuff like registering beans with the agent.
 *
 * @see GemFireHealthJmxImpl#createDistributedSystemHealthConfig
 *
 *
 * @since GemFire 3.5
 */
public class DistributedSystemHealthConfigJmxImpl extends DistributedSystemHealthConfigImpl
    implements ManagedResource {

  /** The <code>GemFireHealth</code> that we help configure */
  private final GemFireHealth health;

  /** The name of the MBean that will manage this resource */
  private final String mbeanName;

  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;

  /** The JMX object name of the MBean for this managed resource */
  private final ObjectName objectName;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>DistributedSystemHealthCOnfigJmxImpl</code> that configures the health of
   * the distributed system monitored by <code>health</code>.
   */
  DistributedSystemHealthConfigJmxImpl(GemFireHealthJmxImpl health) throws AdminException {

    super();
    this.health = health;
    mbeanName =
        new StringBuffer().append(MBEAN_NAME_PREFIX).append("DistributedSystemHealthConfig,id=")
            .append(
                MBeanUtils.makeCompliantMBeanNameProperty(health.getDistributedSystem().getId()))
            .toString();
    objectName = MBeanUtils.createMBean(this);
  }

  ////////////////////// Instance Methods //////////////////////

  /**
   * Applies the changes made to this config back to the health monitor.
   *
   * @see GemFireHealth#setDistributedSystemHealthConfig
   */
  public void applyChanges() {
    health.setDistributedSystemHealthConfig(this);
  }

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
  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.DISTRIBUTED_SYSTEM_HEALTH_CONFIG;
  }

  @Override
  public ObjectName getObjectName() {
    return objectName;
  }

  @Override
  public void cleanupResource() {}

}
