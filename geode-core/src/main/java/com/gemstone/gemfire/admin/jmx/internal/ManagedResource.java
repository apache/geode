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

import javax.management.ObjectName;
import javax.management.modelmbean.ModelMBean;

/**
 * Represents a component or resource that is managed by a 
 * {@link javax.management.modelmbean.ModelMBean}.
 *
 * @since GemFire     3.5
 *
 */
public interface ManagedResource {
  
  /** 
   * The prefix of MBean names. Note: this is NOT used by Members, Stats, or
   * any other MBean that has it's own domain.
   *
   * @see #getMBeanName 
   */
  public static final String MBEAN_NAME_PREFIX = "GemFire:type=";

  /** 
   * Returns the name of the ModelMBean that will manage this
   * resource.  They [some] are of the form
   *
   * <PRE>
   * MBEAN_NAME_PREFIX + typeName + ",id=" + id
   * </PRE>
   *
   * @see #MBEAN_NAME_PREFIX
   */
  public String getMBeanName();
  
  /** Returns the ModelMBean that is configured to manage this resource */
  public ModelMBean getModelMBean();

  /** Sets the ModelMBean that is configured to manage this resource */
  public void setModelMBean(ModelMBean modelMBean);
  
  /** 
   * Returns the enumerated ManagedResourceType of this resource.
   *
   * @see ManagedResourceType
   */
  public ManagedResourceType getManagedResourceType();

  /**
   * Returns the JMX <code>ObjectName</code> of this managed resource.
   *
   * @see #getMBeanName
   */
  public ObjectName getObjectName();
 
  /**
   * Perform any cleanup necessary before stopping management of this resource.
   */
  public void cleanupResource();
  
}

