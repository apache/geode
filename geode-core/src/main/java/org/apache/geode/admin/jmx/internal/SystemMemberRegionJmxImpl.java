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

import org.apache.geode.admin.internal.SystemMemberCacheImpl;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.admin.GemFireVM;

/**
 * MBean representation of {@link org.apache.geode.admin.SystemMemberRegion}.
 *
 * @since GemFire 3.5
 */
public class SystemMemberRegionJmxImpl
    extends org.apache.geode.admin.internal.SystemMemberRegionImpl
    implements org.apache.geode.admin.jmx.internal.ManagedResource {

  /** The object name of this managed resource */
  private ObjectName objectName;

  // -------------------------------------------------------------------------
  // Constructor(s)
  // -------------------------------------------------------------------------

  /**
   * Constructs an instance of SystemMemberRegionJmxImpl.
   *
   * @param cache the cache this region belongs to
   * @param region internal region to delegate real work to
   */
  public SystemMemberRegionJmxImpl(SystemMemberCacheImpl cache, Region region)
      throws org.apache.geode.admin.AdminException {
    super(cache, region);
    initializeMBean(cache);
  }

  /** Create and register the MBean to manage this resource */
  private void initializeMBean(SystemMemberCacheImpl cache)
      throws org.apache.geode.admin.AdminException {

    GemFireVM vm = cache.getVM();
    this.mbeanName = new StringBuffer("GemFire.Cache:").append("path=")
        .append(MBeanUtils.makeCompliantMBeanNameProperty(getFullPath())).append(",name=")
        .append(MBeanUtils.makeCompliantMBeanNameProperty(cache.getName())).append(",id=")
        .append(cache.getId()).append(",owner=")
        .append(MBeanUtils.makeCompliantMBeanNameProperty(vm.getId().toString()))
        .append(",type=Region").toString();

    this.objectName = MBeanUtils.createMBean(this);
  }

  // -------------------------------------------------------------------------
  // ManagedResource implementation
  // -------------------------------------------------------------------------

  /** The name of the MBean that will manage this resource */
  private String mbeanName;

  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;

  @Override
  public String getMBeanName() {
    return this.mbeanName;
  }

  @Override
  public ModelMBean getModelMBean() {
    return this.modelMBean;
  }

  @Override
  public void setModelMBean(ModelMBean modelMBean) {
    this.modelMBean = modelMBean;
  }

  @Override
  public ObjectName getObjectName() {
    return this.objectName;
  }

  @Override
  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.SYSTEM_MEMBER_REGION;
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
    if (!(obj instanceof SystemMemberRegionJmxImpl)) {
      return false;
    }

    SystemMemberRegionJmxImpl other = (SystemMemberRegionJmxImpl) obj;

    return this.getMBeanName().equals(other.getMBeanName());
  }

  /**
   * Returns hash code for <code>this</code> object which is based on the MBean Name generated.
   *
   * @return hash code for <code>this</code> object
   */
  @Override
  public int hashCode() {
    return this.getMBeanName().hashCode();
  }
}
