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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.modeler.AttributeInfo;
import org.apache.commons.modeler.ManagedBean;
import org.apache.commons.modeler.OperationInfo;

/**
 * Extends ManagedBean to allow for dynamically creating new instances of ManagedBean based on an
 * existing instance of ManagedBean.
 *
 * @since GemFire 5.0.1
 */
public class DynamicManagedBean extends org.apache.commons.modeler.ManagedBean {
  private static final long serialVersionUID = 4051924500150228160L;

  public DynamicManagedBean(ManagedBean managed) {
    super();

    attributes = managed.getAttributes();
    className = managed.getClassName();
    constructors = managed.getConstructors();
    description = managed.getDescription();
    domain = managed.getDomain();
    group = managed.getGroup();
    name = managed.getName();
    fields = managed.getFields();
    notifications = managed.getNotifications();
    operations = managed.getOperations();
    type = managed.getType();

    /*
     * we don't use modelerType and it's nice to remove it to keep the list of attributes cleaned
     * up...
     */
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
    synchronized (attributes) {
      List attributesList = new ArrayList(attributes.length);
      for (final AttributeInfo attribute : attributes) {
        if (!name.equals(attribute.getName())) {
          attributesList.add(attribute);
        }
      }
      attributes =
          (AttributeInfo[]) attributesList.toArray(new AttributeInfo[0]);

      /*
       * super.info should be nulled out anytime the structure is changed, such as altering the
       * attributes, operations, or notifications
       *
       * however super.info is private, so we need the following hack to cause the super class to
       * null it out for us...
       */
      setType(type); // causes this in super: "this.info = null;"
    }
  }

  /**
   * Removes the operation with the given name from thie <code>ManageBean</code>'s operation
   * descriptor list.
   *
   * @since GemFire 4.0
   */
  public void removeOperation(String name) {
    if (name == null || name.length() < 1) {
      return;
    }

    synchronized (operations) {
      List operationsList = new ArrayList(operations.length);
      for (final OperationInfo operation : operations) {
        if (!name.equals(operation.getName())) {
          operationsList.add(operation);
        }
      }
      operations =
          (OperationInfo[]) operationsList.toArray(new OperationInfo[0]);

      /*
       * super.info should be nulled out anytime the structure is changed, such as altering the
       * operations, operations, or notifications
       *
       * however super.info is private, so we need the following hack to cause the super class to
       * null it out for us...
       */
      setType(type); // causes this in super: "this.info = null;"
    }
  }

  /**
   * Return a string representation of this managed bean.
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DynamicManagedBean[");
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
