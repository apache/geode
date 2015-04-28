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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.management.*;
import javax.management.modelmbean.*;

/**
 * The JMX "managed resource" that represents the configuration for
 * the health of GemFire.  Basically, it provides the behavior of
 * <code>GemFireHealthConfigImpl</code>, but does some JMX stuff like
 * registering beans with the agent.
 *
 * <P>
 *
 * Unlike other <code>ManagedResource</code>s this class cannot simply
 * subclass <code>GemFireHealthImpl</code> because it instances are
 * serialized and sent to other VMs.  This is problematic because the
 * other VMs most likely do not have JMX classes like
 * <code>ModelMBean</code> on their classpaths.  So, instead we
 * delegate all of the <code>GemFireHealthConfig</code> behavior to
 * another object which IS serialized.
 *
 * @see GemFireHealthJmxImpl#createDistributedSystemHealthConfig
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
@SuppressFBWarnings(justification="This class is deprecated. Also, any further changes so close to the release is inadvisable.") 
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

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>GemFireHealthConfigJmxImpl</code> that
   * configures the health monitoring of components running on the
   * given host.
   */
  GemFireHealthConfigJmxImpl(GemFireHealthJmxImpl health,
                             String hostName)
    throws AdminException {

    this.delegate = new GemFireHealthConfigImpl(hostName);
    this.health = health;
    this.mbeanName = new StringBuffer()
      .append(MBEAN_NAME_PREFIX)
      .append("GemFireHealthConfig,id=")
      .append(MBeanUtil.makeCompliantMBeanNameProperty(health.getDistributedSystem().getId()))
      .append(",host=") 
      .append((hostName == null ? "default" : MBeanUtil.makeCompliantMBeanNameProperty(hostName)))
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
    String hostName = this.getHostName();
    if (hostName == null) {
      this.health.setDefaultGemFireHealthConfig(this);

    } else {
      this.health.setGemFireHealthConfig(hostName, this);
    }
  }

  public String getMBeanName() {
    return this.mbeanName;
  }
  
  public ModelMBean getModelMBean() {
    return this.modelMBean;
  }

  public ObjectName getObjectName() {
    return this.objectName;
  }

  public void setModelMBean(ModelMBean modelMBean) {
    this.modelMBean = modelMBean;
  }

  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.GEMFIRE_HEALTH_CONFIG;
  }

  /**
   * Replace this object with the delegate that can be properly
   * serialized. 
   */
  public Object writeReplace() {
    return this.delegate;
  }

  //////////////////////  MemberHealthConfig  //////////////////////

  public long getMaxVMProcessSize() {
    return delegate.getMaxVMProcessSize();
  }

  public void setMaxVMProcessSize(long size) {
    delegate.setMaxVMProcessSize(size);
  }

  public long getMaxMessageQueueSize() {
    return delegate.getMaxMessageQueueSize();
  }

  public void setMaxMessageQueueSize(long maxMessageQueueSize) {
    delegate.setMaxMessageQueueSize(maxMessageQueueSize);
  }

  public long getMaxReplyTimeouts() {
    return delegate.getMaxReplyTimeouts();
  }

  public void setMaxReplyTimeouts(long maxReplyTimeouts) {
    delegate.setMaxReplyTimeouts(maxReplyTimeouts);
  }

  public double getMaxRetransmissionRatio() {
    return delegate.getMaxRetransmissionRatio();
  }
  
  public void setMaxRetransmissionRatio(double ratio) {
    delegate.setMaxRetransmissionRatio(ratio);
  }

  //////////////////////  CacheHealthConfig  //////////////////////

    public long getMaxNetSearchTime() {
    return delegate.getMaxNetSearchTime();
  }

  public void setMaxNetSearchTime(long maxNetSearchTime) {
    delegate.setMaxNetSearchTime(maxNetSearchTime);
  }

  public long getMaxLoadTime() {
    return delegate.getMaxLoadTime();
  }

  public void setMaxLoadTime(long maxLoadTime) {
    delegate.setMaxLoadTime(maxLoadTime);
  }

  public double getMinHitRatio() {
    return delegate.getMinHitRatio();
  }

  public void setMinHitRatio(double minHitRatio) {
    delegate.setMinHitRatio(minHitRatio);
  }

  public long getMaxEventQueueSize() {
    return delegate.getMaxEventQueueSize();
  }

  public void setMaxEventQueueSize(long maxEventQueueSize) {
    delegate.setMaxEventQueueSize(maxEventQueueSize);
  }

  //////////////////////  GemFireHealthConfig  //////////////////////

  public String getHostName() {
    return delegate.getHostName();
  }

  public void setHealthEvaluationInterval(int interval) {
    delegate.setHealthEvaluationInterval(interval);
  }

  public int getHealthEvaluationInterval() {
    return delegate.getHealthEvaluationInterval();
  }

  public void cleanupResource() {}
  
}
