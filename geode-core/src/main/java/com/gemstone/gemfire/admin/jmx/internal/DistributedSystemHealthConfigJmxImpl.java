/*
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
