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
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.admin.internal.GemFireHealthConfigImpl;

/**
 * The JMX "managed resource" that represents the configuration for the health of GemFire.
 * Basically, it provides the behavior of <code>GemFireHealthConfigImpl</code>, but does some JMX
 * stuff like registering beans with the agent.
 *
 * <P>
 *
 * Unlike other <code>ManagedResource</code>s this class cannot simply subclass
 * <code>GemFireHealthImpl</code> because it instances are serialized and sent to other VMs. This is
 * problematic because the other VMs most likely do not have JMX classes like
 * <code>ModelMBean</code> on their classpaths. So, instead we delegate all of the
 * <code>GemFireHealthConfig</code> behavior to another object which IS serialized.
 *
 * @see GemFireHealthJmxImpl#createDistributedSystemHealthConfig
 *
 *
 * @since GemFire 3.5
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    justification = "This class is deprecated. Also, any further changes so close to the release is inadvisable.")
public class GemFireHealthConfigJmxImpl
    implements GemFireHealthConfig, ManagedResource, java.io.Serializable {

  private static final long serialVersionUID = 1482719647163239953L;

  /** The <code>GemFireHealth</code> that we help configure */
  private GemFireHealth health;

  /** The name of the MBean that will manage this resource */
  private String mbeanName;

  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;

  /** The delegate that contains the real config state */
  private GemFireHealthConfig delegate;

  /** The object name of this managed resource */
  private ObjectName objectName;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>GemFireHealthConfigJmxImpl</code> that configures the health monitoring of
   * components running on the given host.
   */
  GemFireHealthConfigJmxImpl(GemFireHealthJmxImpl health, String hostName) throws AdminException {

    this.delegate = new GemFireHealthConfigImpl(hostName);
    this.health = health;
    this.mbeanName = new StringBuffer().append(MBEAN_NAME_PREFIX).append("GemFireHealthConfig,id=")
        .append(MBeanUtils.makeCompliantMBeanNameProperty(health.getDistributedSystem().getId()))
        .append(",host=")
        .append(
            (hostName == null ? "default" : MBeanUtils.makeCompliantMBeanNameProperty(hostName)))
        .toString();
    this.objectName = MBeanUtils.createMBean(this);
  }

  ////////////////////// Instance Methods //////////////////////

  /**
   * Applies the changes made to this config back to the health monitor.
   *
   * @see GemFireHealth#setDistributedSystemHealthConfig
   */
  public void applyChanges() {
    String hostName = this.getHostName();
    if (hostName == null) {
      this.health.setDefaultGemFireHealthConfig(this);

    } else {
      this.health.setGemFireHealthConfig(hostName, this);
    }
  }

  @Override
  public String getMBeanName() {
    return this.mbeanName;
  }

  @Override
  public ModelMBean getModelMBean() {
    return this.modelMBean;
  }

  @Override
  public ObjectName getObjectName() {
    return this.objectName;
  }

  @Override
  public void setModelMBean(ModelMBean modelMBean) {
    this.modelMBean = modelMBean;
  }

  @Override
  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.GEMFIRE_HEALTH_CONFIG;
  }

  /**
   * Replace this object with the delegate that can be properly serialized.
   */
  public Object writeReplace() {
    return this.delegate;
  }

  ////////////////////// MemberHealthConfig //////////////////////

  @Override
  public long getMaxVMProcessSize() {
    return delegate.getMaxVMProcessSize();
  }

  @Override
  public void setMaxVMProcessSize(long size) {
    delegate.setMaxVMProcessSize(size);
  }

  @Override
  public long getMaxMessageQueueSize() {
    return delegate.getMaxMessageQueueSize();
  }

  @Override
  public void setMaxMessageQueueSize(long maxMessageQueueSize) {
    delegate.setMaxMessageQueueSize(maxMessageQueueSize);
  }

  @Override
  public long getMaxReplyTimeouts() {
    return delegate.getMaxReplyTimeouts();
  }

  @Override
  public void setMaxReplyTimeouts(long maxReplyTimeouts) {
    delegate.setMaxReplyTimeouts(maxReplyTimeouts);
  }

  @Override
  public double getMaxRetransmissionRatio() {
    return delegate.getMaxRetransmissionRatio();
  }

  @Override
  public void setMaxRetransmissionRatio(double ratio) {
    delegate.setMaxRetransmissionRatio(ratio);
  }

  ////////////////////// CacheHealthConfig //////////////////////

  @Override
  public long getMaxNetSearchTime() {
    return delegate.getMaxNetSearchTime();
  }

  @Override
  public void setMaxNetSearchTime(long maxNetSearchTime) {
    delegate.setMaxNetSearchTime(maxNetSearchTime);
  }

  @Override
  public long getMaxLoadTime() {
    return delegate.getMaxLoadTime();
  }

  @Override
  public void setMaxLoadTime(long maxLoadTime) {
    delegate.setMaxLoadTime(maxLoadTime);
  }

  @Override
  public double getMinHitRatio() {
    return delegate.getMinHitRatio();
  }

  @Override
  public void setMinHitRatio(double minHitRatio) {
    delegate.setMinHitRatio(minHitRatio);
  }

  @Override
  public long getMaxEventQueueSize() {
    return delegate.getMaxEventQueueSize();
  }

  @Override
  public void setMaxEventQueueSize(long maxEventQueueSize) {
    delegate.setMaxEventQueueSize(maxEventQueueSize);
  }

  ////////////////////// GemFireHealthConfig //////////////////////

  @Override
  public String getHostName() {
    return delegate.getHostName();
  }

  @Override
  public void setHealthEvaluationInterval(int interval) {
    delegate.setHealthEvaluationInterval(interval);
  }

  @Override
  public int getHealthEvaluationInterval() {
    return delegate.getHealthEvaluationInterval();
  }

  @Override
  public void cleanupResource() {}

}
