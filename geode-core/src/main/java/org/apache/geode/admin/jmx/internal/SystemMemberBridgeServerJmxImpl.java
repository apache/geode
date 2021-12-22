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
import org.apache.geode.admin.internal.SystemMemberBridgeServerImpl;
import org.apache.geode.admin.internal.SystemMemberCacheImpl;
import org.apache.geode.internal.admin.AdminBridgeServer;
import org.apache.geode.internal.admin.GemFireVM;

/**
 * MBean representation of a {@link org.apache.geode.admin.SystemMemberBridgeServer}.
 *
 * @since GemFire 4.0
 */
public class SystemMemberBridgeServerJmxImpl extends SystemMemberBridgeServerImpl
    implements ManagedResource {

  /** The object name of this managed resource */
  private ObjectName objectName;

  /** The name of the MBean that will manage this resource */
  private String mbeanName;

  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;

  ////////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>SystemMemberBridgeServerJmxImpl</code> that serves the contents of the
   * given cache.
   */
  SystemMemberBridgeServerJmxImpl(SystemMemberCacheImpl cache, AdminBridgeServer bridgeInfo)
      throws AdminException {

    super(cache, bridgeInfo);
    initializeMBean(cache);
  }

  ////////////////////// Instance Methods //////////////////////

  /**
   * Creates and registers the MBean to manage this resource
   */
  private void initializeMBean(SystemMemberCacheImpl cache) throws AdminException {

    GemFireVM vm = cache.getVM();
    mbeanName = new StringBuilder("GemFire.Cache:").append("name=")
        .append(MBeanUtils.makeCompliantMBeanNameProperty(cache.getName())).append(",id=")
        .append(getBridgeId()).append(",owner=")
        .append(MBeanUtils.makeCompliantMBeanNameProperty(vm.getId().toString()))
        .append(",type=CacheServer").toString();
    objectName = MBeanUtils.createMBean(this);
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
  public ObjectName getObjectName() {
    return objectName;
  }

  @Override
  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.SYSTEM_MEMBER_CACHE_SERVER;
  }

  @Override
  public void cleanupResource() {}

  /**
   * Checks equality of the given object with <code>this</code> based on the type (Class) and the
   * MBean Name returned by <code>getMBeanName()</code> methods.
   *
   * @param obj object to check equality with
   * @return true if the given object is if the same type and its MBean Name is same as
   *         <code>this</code> object's MBean Name, false otherwise
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof SystemMemberBridgeServerJmxImpl)) {
      return false;
    }

    SystemMemberBridgeServerJmxImpl other = (SystemMemberBridgeServerJmxImpl) obj;

    return getMBeanName().equals(other.getMBeanName());
  }

  /**
   * Returns hash code for <code>this</code> object which is based on the MBean Name generated.
   *
   * @return hash code for <code>this</code> object
   */
  @Override
  public int hashCode() {
    return getMBeanName().hashCode();
  }
}
