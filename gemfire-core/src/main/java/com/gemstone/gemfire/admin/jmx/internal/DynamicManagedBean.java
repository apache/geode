/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.admin.jmx.internal;


import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.modeler.AttributeInfo;
import org.apache.commons.modeler.OperationInfo;
import org.apache.commons.modeler.ManagedBean;

/**
 * Extends ManagedBean to allow for dynamically creating new instances of
 * ManagedBean based on an existing instance of ManagedBean.
 * 
 * @author  Kirk Lund
 * @since 5.0.1
 */
public class DynamicManagedBean extends org.apache.commons.modeler.ManagedBean {
  private static final long serialVersionUID = 4051924500150228160L;
  
  public DynamicManagedBean(ManagedBean managed) {
    super();
    
    this.attributes = managed.getAttributes();
    this.className = managed.getClassName();
    this.constructors = managed.getConstructors();
    this.description = managed.getDescription();
    this.domain = managed.getDomain();
    this.group = managed.getGroup();
    this.name = managed.getName();
    this.fields = managed.getFields();
    this.notifications = managed.getNotifications();
    this.operations = managed.getOperations();
    this.type = managed.getType();
    
    /* we don't use modelerType and it's nice to remove it to keep the list of 
       attributes cleaned up...*/
    removeAttribute("modelerType");
  }

  /**
   * Removes an attribute from this ManagedBean's attribute descriptor list.
   *
   * @param name the attribute to be removed
   */
  public void removeAttribute(String name) {
    if (name == null || name.length() < 1) {
      return;
    }
    synchronized (this.attributes) {
      List attributesList = new ArrayList(this.attributes.length);
      for (int i = 0; i < this.attributes.length; i++) {
        if (!name.equals(this.attributes[i].getName())) {
          attributesList.add(this.attributes[i]);
        }
      }
      this.attributes = (AttributeInfo[]) 
          attributesList.toArray(new AttributeInfo[attributesList.size()]);
      
      /* super.info should be nulled out anytime the structure is changed,
       * such as altering the attributes, operations, or notifications
       *
       * however super.info is private, so we need the following hack to cause
       * the super class to null it out for us...
       */
      setType(this.type); // causes this in super: "this.info = null;"
    }
  }

  /**
   * Removes the operation with the given name from thie
   * <code>ManageBean</code>'s operation descriptor list.
   *
   * @since 4.0
   */
  public void removeOperation(String name) {
    if (name == null || name.length() < 1) {
      return;
    }

    synchronized (operations) {
      List operationsList = new ArrayList(this.operations.length);
      for (int i = 0; i < this.operations.length; i++) {
        if (!name.equals(this.operations[i].getName())) {
          operationsList.add(this.operations[i]);
        }
      }
      this.operations = (OperationInfo[]) 
          operationsList.toArray(new OperationInfo[operationsList.size()]);
      
      /* super.info should be nulled out anytime the structure is changed,
       * such as altering the operations, operations, or notifications
       *
       * however super.info is private, so we need the following hack to cause
       * the super class to null it out for us...
       */
      setType(this.type); // causes this in super: "this.info = null;"
    }
  }

  /**
   * Return a string representation of this managed bean.
   */
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer("DynamicManagedBean[");
    sb.append("name=");
    sb.append(name);
    sb.append(", className=");
    sb.append(className);
    sb.append(", description=");
    sb.append(description);
    if (group != null) {
      sb.append(", group=");
      sb.append(group);
    }
    sb.append(", type=");
    sb.append(type);
    sb.append(", attributes=");
    sb.append(Arrays.asList(attributes));
    sb.append("]");
    return (sb.toString());
  }
}

