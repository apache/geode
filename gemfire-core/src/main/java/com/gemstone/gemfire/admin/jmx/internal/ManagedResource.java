/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.admin.jmx.internal;

import javax.management.ObjectName;
import javax.management.modelmbean.ModelMBean;

/**
 * Represents a component or resource that is managed by a 
 * {@link javax.management.modelmbean.ModelMBean}.
 *
 * @author    Kirk Lund
 * @since     3.5
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

