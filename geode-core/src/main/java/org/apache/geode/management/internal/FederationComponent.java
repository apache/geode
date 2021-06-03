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
package org.apache.geode.management.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import javax.management.ObjectName;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Central component for federation It consists of an Object State as well as some meta data for the
 * Object being federated.
 *
 *
 */

public class FederationComponent
    implements java.io.Serializable, DataSerializableFixedID {
  private static final Logger logger = LogService.getLogger();

  private static final String THIS_COMPONENT = FederationComponent.class.getName();
  private static final long serialVersionUID = 3123549507449088591L;

  /**
   * Name of the MBean. This name will be replicated at Managing Node
   */
  private String objectName;

  /**
   * Name if the interface class . It will determine the interface for MBean at Managing Node side
   */
  private String interfaceClassName;

  /**
   * Flag to determine if MBean emits notification or not.
   */
  private boolean notificationEmitter;

  /**
   * This Map holds the object state as property-value Every component should be serializable
   */
  private Map<String, Object> objectState = new HashMap<String, Object>();

  private transient Map<String, Method> getterMethodMap;

  private transient Object mbeanObject;

  private transient Class mbeanInterfaceClass;


  private transient Map<String, Object> oldObjectState = new HashMap<String, Object>();

  private final transient Map<Method, OpenMethod> methodHandlerMap = OpenTypeUtil.newMap();

  private transient boolean prevRefreshChangeDetected = false;

  /**
   *
   * @param objectName ObjectName of the MBean
   * @param interfaceClass interface class of the MBean
   * @param notificationEmitter specifies whether this MBean is going to emit notifications
   */
  public FederationComponent(Object object, ObjectName objectName, Class interfaceClass,
      boolean notificationEmitter) {
    this.objectName = objectName.toString();
    this.interfaceClassName = interfaceClass.getCanonicalName();
    this.mbeanInterfaceClass = interfaceClass;
    this.notificationEmitter = notificationEmitter;
    this.mbeanObject = object;
    getterMethodMap = new HashMap<String, Method>();
    initGetters(interfaceClass);

  }

  public FederationComponent() {}

  // Introspect the mbeanInterface and initialize this object's maps.
  //
  private void initGetters(Class<?> mbeanInterface) {
    final Method[] methodArray = mbeanInterface.getMethods();

    for (Method m : methodArray) {
      String name = m.getName();

      String attrName = "";
      if (name.startsWith("get")) {
        attrName = name.substring(3);
      } else if (name.startsWith("is") && m.getReturnType() == boolean.class) {
        attrName = name.substring(2);
      }

      if (attrName.length() != 0 && m.getParameterTypes().length == 0
          && m.getReturnType() != void.class) { // For Getters
        m.setAccessible(true);
        getterMethodMap.put(attrName, m);
        methodHandlerMap.put(m, OpenMethod.from(m));
      }
    }
  }

  /**
   * gets the Canonical name of the MBean interface
   *
   * @return mbean interface class name
   */

  public String getMBeanInterfaceClass() {
    return interfaceClassName;
  }

  /**
   * True if this MBean is a notification emitter.
   *
   * @return whether its a notification emitter or not
   */

  public boolean isNotificationEmitter() {
    return notificationEmitter;
  }

  /**
   * This method will get called from Management Thread. This will dynamically invoke the MBeans
   * getter methods and set them in ObjectState Map.
   *
   * In Future releases we can implement the delta propagation here
   *
   * @return true if the refresh detects that the state changed. It will return false if two
   *         consecutive refresh calls results in no state change. This indicates to the
   *         LocalManager whether to send the MBean state to Manager or not.
   */

  public boolean refreshObjectState(boolean keepOldState) {
    boolean changeDetected = false;
    Object[] args = null;
    if (keepOldState) {
      oldObjectState.putAll(objectState);
    }
    for (Map.Entry<String, Method> gettorMethodEntry : getterMethodMap.entrySet()) {
      String property = gettorMethodEntry.getKey();
      Object propertyValue = null;

      try {
        Method m = gettorMethodEntry.getValue();
        propertyValue = m.invoke(mbeanObject, args);


        // To Handle open types in getter values
        OpenMethod op = methodHandlerMap.get(m);
        propertyValue = op.toOpenReturnValue(propertyValue);

      } catch (Exception e) {
        propertyValue = null;
        if (logger.isTraceEnabled()) {
          logger.trace(e.getMessage());
        }

      }

      Object oldValue = objectState.put(property, propertyValue);
      if (!changeDetected) {
        if (propertyValue != null) {
          if (!propertyValue.equals(oldValue)) {
            changeDetected = true;
          }
        } else { // new value is null
          if (oldValue != null) {
            changeDetected = true;
          }
        }
      }
    }
    boolean retVal = prevRefreshChangeDetected || changeDetected;
    prevRefreshChangeDetected = changeDetected;
    return retVal;
  }


  public boolean equals(Object anObject) {

    if (this == anObject) {
      return true;
    }
    if (anObject instanceof FederationComponent) {
      FederationComponent anotherFedComp = (FederationComponent) anObject;
      if (anotherFedComp.interfaceClassName.equals(this.interfaceClassName)
          && anotherFedComp.notificationEmitter == this.notificationEmitter
          && anotherFedComp.objectState.equals(this.objectState)
          && anotherFedComp.objectName.equals(this.objectName)) {
        return true;
      }
    }

    return false;
  }


  public int hashCode() {
    return objectName.hashCode();
  }

  /**
   * Managing node will get Object state by calling this method
   *
   * @return value of the given property
   */
  public Object getValue(String propertyName) {
    return objectState.get(propertyName);
  }

  public String toString() {
    if (Boolean.getBoolean("debug.Management")) {
      return " ObjectName = " + objectName + ",InterfaceClassName = " + interfaceClassName
          + ", NotificationEmitter = " + notificationEmitter + ", ObjectState = "
          + objectState.toString();
    } else {
      return "ObjectName = " + objectName;
    }
  }

  public Map<String, Object> getObjectState() {
    return objectState;
  }

  public Map<String, Object> getOldState() {
    return oldObjectState;
  }


  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    this.notificationEmitter = DataSerializer.readPrimitiveBoolean(in);
    this.interfaceClassName = DataSerializer.readString(in);
    this.objectState = DataSerializer.readHashMap(in);
    this.objectName = DataSerializer.readString(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {

    DataSerializer.writePrimitiveBoolean(this.notificationEmitter, out);
    DataSerializer.writeString(this.interfaceClassName, out);
    DataSerializer.writeHashMap((HashMap<?, ?>) objectState, out);
    DataSerializer.writeString(this.objectName, out);
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.MGMT_FEDERATION_COMPONENT;
  }

  public Object getMBeanObject() {
    return mbeanObject;
  }

  public Class getInterfaceClass() {
    return mbeanInterfaceClass;
  }


  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }


}
