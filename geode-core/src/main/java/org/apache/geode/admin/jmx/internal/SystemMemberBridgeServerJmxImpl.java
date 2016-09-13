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

import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.internal.SystemMemberBridgeServerImpl;
import com.gemstone.gemfire.admin.internal.SystemMemberCacheImpl;
import com.gemstone.gemfire.internal.admin.AdminBridgeServer;
import com.gemstone.gemfire.internal.admin.GemFireVM;

import javax.management.ObjectName;
import javax.management.modelmbean.ModelMBean;

/**
 * MBean representation of a {@link
 * com.gemstone.gemfire.admin.SystemMemberBridgeServer}. 
 *
 * @since GemFire 4.0
 */
public class SystemMemberBridgeServerJmxImpl
  extends SystemMemberBridgeServerImpl implements ManagedResource {

  /** The object name of this managed resource */
  private ObjectName objectName;

  /** The name of the MBean that will manage this resource */
  private String mbeanName;

  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>SystemMemberBridgeServerJmxImpl</code> that
   * serves the contents of the given cache.
   */
  SystemMemberBridgeServerJmxImpl(SystemMemberCacheImpl cache,
                                  AdminBridgeServer bridgeInfo) 
    throws AdminException {

    super(cache, bridgeInfo);
    initializeMBean(cache);
  }

  //////////////////////  Instance Methods  //////////////////////

  /** 
   * Creates and registers the MBean to manage this resource
   */
  private void initializeMBean(SystemMemberCacheImpl cache)
    throws AdminException {
    
    GemFireVM vm = cache.getVM();
    this.mbeanName = new StringBuffer("GemFire.Cache:")
        .append("name=")
        .append(MBeanUtil.makeCompliantMBeanNameProperty(cache.getName()))
        .append(",id=")
        .append(this.getBridgeId())
        .append(",owner=")
        .append(MBeanUtil.makeCompliantMBeanNameProperty(vm.getId().toString()))
        .append(",type=CacheServer").toString();
      
    this.objectName = MBeanUtil.createMBean(this);
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
  
  public ObjectName getObjectName() {
    return this.objectName;
  }

  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.SYSTEM_MEMBER_CACHE_SERVER;
  }
  
  public void cleanupResource() {}
  
  /**
   * Checks equality of the given object with <code>this</code> based on the
   * type (Class) and the MBean Name returned by <code>getMBeanName()</code>
   * methods.
   * 
   * @param obj
   *          object to check equality with
   * @return true if the given object is if the same type and its MBean Name is
   *         same as <code>this</code> object's MBean Name, false otherwise
   */
  @Override
  public boolean equals(Object obj) {
    if ( !(obj instanceof SystemMemberBridgeServerJmxImpl) ) {
      return false;
    }
    
    SystemMemberBridgeServerJmxImpl other = 
                                (SystemMemberBridgeServerJmxImpl) obj; 
    
    return this.getMBeanName().equals(other.getMBeanName());
  }

  /**
   * Returns hash code for <code>this</code> object which is based on the MBean 
   * Name generated. 
   * 
   * @return hash code for <code>this</code> object
   */
  @Override
  public int hashCode() {
    return this.getMBeanName().hashCode();
  }
}
