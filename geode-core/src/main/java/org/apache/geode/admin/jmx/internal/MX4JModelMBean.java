/*
 * Copyright (C) MX4J. All rights reserved.
 *
 * This software is distributed under the terms of the MX4J License version 1.0. See the terms of
 * the MX4J License in the documentation provided with this software.
 */

package org.apache.geode.admin.jmx.internal;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.Iterator;

import javax.management.Attribute;
import javax.management.AttributeChangeNotification;
import javax.management.AttributeChangeNotificationFilter;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.Descriptor;
import javax.management.InstanceNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanRegistration;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationEmitter;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeErrorException;
import javax.management.RuntimeOperationsException;
import javax.management.ServiceNotFoundException;
import javax.management.loading.ClassLoaderRepository;
import javax.management.modelmbean.InvalidTargetObjectTypeException;
import javax.management.modelmbean.ModelMBean;
import javax.management.modelmbean.ModelMBeanAttributeInfo;
import javax.management.modelmbean.ModelMBeanInfo;
import javax.management.modelmbean.ModelMBeanOperationInfo;

import mx4j.ImplementationException;
import mx4j.log.FileLogger;
import mx4j.log.Log;
import mx4j.log.Logger;
import mx4j.log.MBeanLogger;
import mx4j.persist.FilePersister;
import mx4j.persist.MBeanPersister;
import mx4j.persist.PersisterMBean;
import mx4j.util.Utils;


/**
 * @author <a href="mailto:biorn_steedom@users.sourceforge.net">Simone Bordet</a>
 * @version $Revision: 1.14 $
 */
public class MX4JModelMBean implements ModelMBean, MBeanRegistration, NotificationEmitter {
  private static final String OBJECT_RESOURCE_TYPE = "ObjectReference";

  private static final int ALWAYS_STALE = 1;
  private static final int NEVER_STALE = 2;
  private static final int STALE = 3;
  private static final int NOT_STALE = 4;

  private static final int PERSIST_NEVER = -1;
  private static final int PERSIST_ON_TIMER = -2;
  private static final int PERSIST_ON_UPDATE = -3;
  private static final int PERSIST_NO_MORE_OFTEN_THAN = -4;

  private MBeanServer m_mbeanServer;
  private Object m_managedResource;
  private boolean m_canBeRegistered;
  private ModelMBeanInfo m_modelMBeanInfo;
  private NotificationBroadcasterSupport m_attributeChangeBroadcaster =
      new NotificationBroadcasterSupport();
  private NotificationBroadcasterSupport m_generalBroadcaster =
      new NotificationBroadcasterSupport();

  public MX4JModelMBean() throws MBeanException, RuntimeOperationsException {
    try {
      load();
    } catch (Exception x) {
      Logger logger = getLogger();
      logger.warn("Cannot restore previously saved status", x);
    }
  }

  public MX4JModelMBean(ModelMBeanInfo info) throws MBeanException, RuntimeOperationsException {
    if (info == null)
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "ModelMBeanInfo parameter cannot be null."));
    else
      setModelMBeanInfo(info);
  }

  private Logger getLogger() {
    return Log.getLogger(getClass().getName());
  }

  public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception {
    if (m_canBeRegistered) {
      m_mbeanServer = server;
      return name;
    } else {
      throw new MBeanRegistrationException(new IllegalStateException(
          "ModelMBean cannot be registered until setModelMBeanInfo has been called."));
    }
  }

  public void postRegister(Boolean registrationDone) {
    if (!registrationDone.booleanValue())
      clear();
  }

  public void preDeregister() throws Exception {}

  public void postDeregister() {
    clear();
  }

  private void clear() {
    m_mbeanServer = null;
    m_managedResource = null;
    m_modelMBeanInfo = null;
    m_generalBroadcaster = null;
    m_attributeChangeBroadcaster = null;
    // PENDING: also remove generic listeners, attribute change listeners, log4j appenders...
  }

  public void setModelMBeanInfo(ModelMBeanInfo modelMBeanInfo)
      throws MBeanException, RuntimeOperationsException {
    if (modelMBeanInfo == null)
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "ModelMBeanInfo cannot be null."));
    if (!isModelMBeanInfoValid(modelMBeanInfo))
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "ModelMBeanInfo is invalid."));

    m_modelMBeanInfo = (ModelMBeanInfo) modelMBeanInfo.clone();

    Logger logger = getLogger();
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("ModelMBeanInfo successfully set to: " + m_modelMBeanInfo);
    // Only now the MBean can be registered in the MBeanServer
    m_canBeRegistered = true;
  }

  private boolean isModelMBeanInfoValid(ModelMBeanInfo info) {
    if (info == null || info.getClassName() == null)
      return false;
    // PENDING: maybe more checks are needed
    return true;
  }

  public void setManagedResource(Object resource, String resourceType) throws MBeanException,
      RuntimeOperationsException, InstanceNotFoundException, InvalidTargetObjectTypeException {
    if (resource == null)
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "Managed resource cannot be null."));
    if (!isResourceTypeSupported(resourceType))
      throw new InvalidTargetObjectTypeException(resourceType);

    Logger logger = getLogger();
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Setting managed resource to be: " + resource);
    m_managedResource = resource;
  }

  private boolean isResourceTypeSupported(String resourceType) {
    // For now only object reference is supported
    return OBJECT_RESOURCE_TYPE.equals(resourceType);
  }

  private Object getManagedResource() {
    return m_managedResource;
  }

  public MBeanInfo getMBeanInfo() {
    return m_modelMBeanInfo == null ? null : (MBeanInfo) m_modelMBeanInfo.clone();
  }

  public void addAttributeChangeNotificationListener(NotificationListener listener,
      String attributeName, Object handback)
      throws MBeanException, RuntimeOperationsException, IllegalArgumentException {
    if (listener == null)
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "Listener cannot be null."));
    AttributeChangeNotificationFilter filter = new AttributeChangeNotificationFilter();
    if (attributeName != null) {
      filter.enableAttribute(attributeName);
    } else {
      MBeanAttributeInfo[] ai = m_modelMBeanInfo.getAttributes();
      for (int i = 0; i < ai.length; i++) {
        Descriptor d = ((ModelMBeanAttributeInfo) ai[i]).getDescriptor();
        filter.enableAttribute((String) d.getFieldValue("name"));
      }
    }

    getAttributeChangeBroadcaster().addNotificationListener(listener, filter, handback);

    Logger logger = getLogger();
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Listener " + listener + " for attribute " + attributeName
          + " added successfully, handback is " + handback);
  }

  public void addNotificationListener(NotificationListener listener, NotificationFilter filter,
      Object handback) throws IllegalArgumentException {
    m_generalBroadcaster.addNotificationListener(listener, filter, handback);
  }

  public MBeanNotificationInfo[] getNotificationInfo() {
    return m_modelMBeanInfo.getNotifications();
  }

  public void removeAttributeChangeNotificationListener(NotificationListener listener,
      String attributeName) throws RuntimeOperationsException, ListenerNotFoundException {
    try {
      removeAttributeChangeNotificationListener(listener, attributeName, null);
    } catch (MBeanException e) {
      throw new RuntimeOperationsException(new RuntimeException(e.getMessage()));
    }
  }

  // Not in the spec but needed
  private void removeAttributeChangeNotificationListener(NotificationListener listener,
      String attributeName, Object handback)
      throws MBeanException, RuntimeOperationsException, ListenerNotFoundException {
    if (listener == null)
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "Listener cannot be null."));
    AttributeChangeNotificationFilter filter = new AttributeChangeNotificationFilter();
    if (attributeName != null) {
      filter.enableAttribute(attributeName);
    } else {
      MBeanAttributeInfo[] ai = m_modelMBeanInfo.getAttributes();
      for (int i = 0; i < ai.length; i++) {
        Descriptor d = ((ModelMBeanAttributeInfo) ai[i]).getDescriptor();
        filter.enableAttribute((String) d.getFieldValue("name"));
      }
    }

    getAttributeChangeBroadcaster().removeNotificationListener(listener, filter, handback);

    Logger logger = getLogger();
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Listener " + listener + " for attribute " + attributeName
          + " removed successfully, handback is " + handback);
  }

  public void removeNotificationListener(NotificationListener listener)
      throws RuntimeOperationsException, ListenerNotFoundException {
    m_generalBroadcaster.removeNotificationListener(listener);
  }

  public void removeNotificationListener(NotificationListener listener, NotificationFilter filter,
      Object handback) throws RuntimeOperationsException, ListenerNotFoundException {
    m_generalBroadcaster.removeNotificationListener(listener, filter, handback);
  }

  public void sendAttributeChangeNotification(Attribute oldAttribute, Attribute newAttribute)
      throws MBeanException, RuntimeOperationsException {
    if (oldAttribute == null || newAttribute == null)
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "Attribute cannot be null."));
    if (!oldAttribute.getName().equals(newAttribute.getName()))
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "Attribute names cannot be different."));

    // TODO: the source must be the object name of the MBean if the listener was registered through
    // MBeanServer
    Object oldValue = oldAttribute.getValue();
    AttributeChangeNotification n = new AttributeChangeNotification(this, 1,
        System.currentTimeMillis(), "Attribute value changed", oldAttribute.getName(),
        oldValue == null ? null : oldValue.getClass().getName(), oldValue, newAttribute.getValue());
    sendAttributeChangeNotification(n);
  }

  public void sendAttributeChangeNotification(AttributeChangeNotification notification)
      throws MBeanException, RuntimeOperationsException {
    if (notification == null)
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "Notification cannot be null."));

    getAttributeChangeBroadcaster().sendNotification(notification);

    Logger modelMBeanLogger = getModelMBeanLogger(notification.getType());
    if (modelMBeanLogger != null)
      if (modelMBeanLogger.isEnabledFor(Logger.DEBUG))
        modelMBeanLogger.debug("ModelMBean log: " + new Date() + " - " + notification);

    Logger logger = getLogger();
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Attribute change notification " + notification + " sent");
  }

  public void sendNotification(String message) throws MBeanException, RuntimeOperationsException {
    Notification notification = new Notification("jmx.modelmbean.general", this, 1, message);
    sendNotification(notification);
  }

  public void sendNotification(Notification notification)
      throws MBeanException, RuntimeOperationsException {
    if (m_generalBroadcaster != null) {
      m_generalBroadcaster.sendNotification(notification);
    }
  }

  public AttributeList getAttributes(String[] attributes) {
    if (attributes == null)
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "Attribute names cannot be null."));

    Logger logger = getLogger();

    AttributeList list = new AttributeList();
    for (int i = 0; i < attributes.length; ++i) {
      String attrName = attributes[i];
      Attribute attribute = null;
      try {
        Object value = getAttribute(attrName);
        attribute = new Attribute(attrName, value);
        list.add(attribute);
      } catch (Exception x) {
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("getAttribute for attribute " + attrName + " failed", x);
        // And go on with the next attribute
      }
    }
    return list;
  }

  public Object getAttribute(String attribute)
      throws AttributeNotFoundException, MBeanException, ReflectionException {
    if (attribute == null)
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "Attribute name cannot be null."));

    Logger logger = getLogger();

    // I want the real info, not its clone
    ModelMBeanInfo info = getModelMBeanInfo();
    if (info == null)
      throw new AttributeNotFoundException(
          "ModelMBeanInfo is null");
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("ModelMBeanInfo is: " + info);

    // This is a clone, we use it read only
    ModelMBeanAttributeInfo attrInfo = info.getAttribute(attribute);
    if (attrInfo == null)
      throw new AttributeNotFoundException(
          String.format("Cannot find ModelMBeanAttributeInfo for attribute %s",
              attribute));
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Attribute info is: " + attrInfo);
    if (!attrInfo.isReadable())
      throw new AttributeNotFoundException(
          String.format("Attribute %s is not readable", attribute));

    // This returns a clone of the mbean descriptor, we use it read only
    Descriptor mbeanDescriptor = info.getMBeanDescriptor();
    if (mbeanDescriptor == null)
      throw new AttributeNotFoundException(
          "MBean descriptor cannot be null");
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("MBean descriptor is: " + mbeanDescriptor);

    // This descriptor is a clone
    Descriptor attributeDescriptor = attrInfo.getDescriptor();
    if (attributeDescriptor == null)
      throw new AttributeNotFoundException(
          String.format("Attribute descriptor for attribute %s cannot be null",
              attribute));
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Attribute descriptor is: " + attributeDescriptor);

    Object returnValue = null;

    String lastUpdateField = "lastUpdatedTimeStamp";

    int staleness = getStaleness(attributeDescriptor, mbeanDescriptor, lastUpdateField);

    if (staleness == ALWAYS_STALE || staleness == STALE) {
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("Value is stale");

      String getter = (String) attributeDescriptor.getFieldValue("getMethod");
      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("getMethod field is: " + getter);
      if (getter == null) {
        // No getter, use default value
        returnValue = attributeDescriptor.getFieldValue("default");

        if (returnValue != null) {
          // Check if the return type is of the same type
          // As an extension allow covariant return type
          Class returned = returnValue.getClass();
          Class declared = loadClassWithContextClassLoader(attrInfo.getType());

          checkAssignability(returned, declared);
        }

        if (logger.isEnabledFor(Logger.DEBUG))
          logger.debug(
              "getAttribute for attribute " + attribute + " returns default value: " + returnValue);
      } else {
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("Invoking attribute getter...");
        // As an extension, allow attributes to be called on target objects also
        Object target = resolveTargetObject(attributeDescriptor);
        returnValue = invokeMethod(target, getter, new Class[0], new Object[0]);
        if (logger.isEnabledFor(Logger.DEBUG))
          logger.debug("Returned value is: " + returnValue);

        if (returnValue != null) {
          // Check if the return type is of the same type
          // As an extension allow covariant return type
          Class returned = returnValue.getClass();
          Class declared = loadClassWithContextClassLoader(attrInfo.getType());

          checkAssignability(returned, declared);
        }

        // Cache the new value only if caching is needed
        if (staleness != ALWAYS_STALE) {
          attributeDescriptor.setField("value", returnValue);
          attributeDescriptor.setField(lastUpdateField, Long.valueOf(System.currentTimeMillis()));
          if (logger.isEnabledFor(Logger.TRACE))
            logger.trace("Returned value has been cached");

          // And now replace the descriptor with the updated clone
          info.setDescriptor(attributeDescriptor, "attribute");
        }

        if (logger.isEnabledFor(Logger.DEBUG))
          logger.debug(
              "getAttribute for attribute " + attribute + " returns invoked value: " + returnValue);
      }
    } else {
      // Return cached value
      returnValue = attributeDescriptor.getFieldValue("value");

      if (returnValue != null) {
        // Check if the return type is of the same type
        // As an extension allow covariant return type
        Class returned = returnValue.getClass();
        Class declared = loadClassWithContextClassLoader(attrInfo.getType());

        checkAssignability(returned, declared);
      }

      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug(
            "getAttribute for attribute " + attribute + " returns cached value: " + returnValue);
    }

    // Puff, everything went ok
    return returnValue;
  }

  public AttributeList setAttributes(AttributeList attributes) {
    if (attributes == null)
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "Attribute list cannot be null."));

    Logger logger = getLogger();

    AttributeList list = new AttributeList();
    for (Iterator i = attributes.iterator(); i.hasNext();) {
      Attribute attribute = (Attribute) i.next();
      String name = attribute.getName();
      try {
        setAttribute(attribute);
        list.add(attribute);
      } catch (Exception x) {
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("setAttribute for attribute " + name + " failed", x);
        // And go on with the next one
      }
    }
    return list;
  }

  public void setAttribute(Attribute attribute) throws AttributeNotFoundException,
      InvalidAttributeValueException, MBeanException, ReflectionException {
    if (attribute == null)
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "Attribute cannot be null."));

    Logger logger = getLogger();

    // No need to synchronize: I work mostly on clones
    // I want the real info, not its clone
    ModelMBeanInfo info = getModelMBeanInfo();
    if (info == null)
      throw new AttributeNotFoundException(
          "ModelMBeanInfo is null");
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("ModelMBeanInfo is: " + info);

    String attrName = attribute.getName();
    Object attrValue = attribute.getValue();

    // This is a clone, we use it read only
    ModelMBeanAttributeInfo attrInfo = info.getAttribute(attrName);
    if (attrInfo == null)
      throw new AttributeNotFoundException(
          String.format("Cannot find ModelMBeanAttributeInfo for attribute %s",
              attrName));
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Attribute info is: " + attrInfo);

    if (!attrInfo.isWritable())
      throw new AttributeNotFoundException(
          String.format("Attribute %s is not writable", attrName));

    // This returns a clone of the mbean descriptor, we use it read only
    Descriptor mbeanDescriptor = info.getMBeanDescriptor();
    if (mbeanDescriptor == null)
      throw new AttributeNotFoundException(
          "MBean descriptor cannot be null");
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("MBean descriptor is: " + mbeanDescriptor);

    // This descriptor is a clone
    Descriptor attributeDescriptor = attrInfo.getDescriptor();
    if (attributeDescriptor == null)
      throw new AttributeNotFoundException(
          String.format("Attribute descriptor for attribute %s cannot be null",
              attrName));
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Attribute descriptor is: " + attributeDescriptor);

    String lastUpdateField = "lastUpdatedTimeStamp";

    Object oldValue = null;
    try {
      oldValue = getAttribute(attrName);
      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("Previous value of attribute " + attrName + ": " + oldValue);
    } catch (Exception x) {
      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("Cannot get previous value of attribute " + attrName, x);
    }

    // Check if setMethod is present
    String method = (String) attributeDescriptor.getFieldValue("setMethod");
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("setMethod field is: " + method);
    if (method != null) {
      Class declared = loadClassWithContextClassLoader(attrInfo.getType());
      if (attrValue != null) {
        Class parameter = attrValue.getClass();
        checkAssignability(parameter, declared);
      }

      // As an extension, allow attributes to be called on target objects also
      Object target = resolveTargetObject(attributeDescriptor);
      invokeMethod(target, method, new Class[] {declared}, new Object[] {attrValue});

      // Cache the value only if currencyTimeLimit is not 0, ie it is not always stale
      int staleness = getStaleness(attributeDescriptor, mbeanDescriptor, lastUpdateField);
      if (staleness != ALWAYS_STALE) {
        attributeDescriptor.setField("value", attrValue);
        attributeDescriptor.setField(lastUpdateField, Long.valueOf(System.currentTimeMillis()));
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("Attribute's value has been cached");
      } else {
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("Always stale, avoiding to cache attribute's value");
      }
    } else {
      if (attrValue != null) {
        Class parameter = attrValue.getClass();
        Class declared = loadClassWithContextClassLoader(attrInfo.getType());

        checkAssignability(parameter, declared);
      }

      // Always store the value in the descriptor: no setMethod
      attributeDescriptor.setField("value", attrValue);
    }

    // And now replace the descriptor with the updated clone
    info.setDescriptor(attributeDescriptor, "attribute");

    // Send notifications to listeners
    if (logger.isEnabledFor(Logger.TRACE))
      logger.trace("Sending attribute change notifications");
    sendAttributeChangeNotification(new Attribute(attrName, oldValue), attribute);

    // Persist this ModelMBean
    boolean persistNow = shouldPersistNow(attributeDescriptor, mbeanDescriptor, lastUpdateField);
    if (persistNow) {
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("Persisting this ModelMBean...");
      try {
        store();
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("ModelMBean persisted successfully");
      } catch (Exception x) {
        logger.error("Cannot store ModelMBean after setAttribute", x);
        if (x instanceof MBeanException)
          throw (MBeanException) x;
        else
          throw new MBeanException(x);
      }
    }
  }

  public Object invoke(String method, Object[] arguments, String[] params)
      throws MBeanException, ReflectionException {
    if (method == null)
      throw new RuntimeOperationsException(new IllegalArgumentException(
          "Method name cannot be null."));
    if (arguments == null)
      arguments = new Object[0];
    if (params == null)
      params = new String[0];

    Logger logger = getLogger();

    // Find operation descriptor
    ModelMBeanInfo info = getModelMBeanInfo();
    if (info == null)
      throw new MBeanException(new ServiceNotFoundException(
          "ModelMBeanInfo is null"));
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("ModelMBeanInfo is: " + info);

    // This is a clone, we use it read only
    ModelMBeanOperationInfo operInfo = info.getOperation(method);
    if (operInfo == null)
      throw new MBeanException(new ServiceNotFoundException(
          String.format("Cannot find ModelMBeanOperationInfo for operation %s",
              method)));
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Operation info is: " + operInfo);

    // This descriptor is a clone
    Descriptor operationDescriptor = operInfo.getDescriptor();
    if (operationDescriptor == null)
      throw new MBeanException(new ServiceNotFoundException(
          String.format("Operation descriptor for operation %s cannot be null",
              method)));
    String role = (String) operationDescriptor.getFieldValue("role");
    if (role == null || !role.equals("operation"))
      throw new MBeanException(new ServiceNotFoundException(
          String.format("Operation descriptor field 'role' must be 'operation', not %s",
              role)));
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Operation descriptor is: " + operationDescriptor);

    // This returns a clone of the mbean descriptor, we use it read only
    Descriptor mbeanDescriptor = info.getMBeanDescriptor();
    if (mbeanDescriptor == null)
      throw new MBeanException(new ServiceNotFoundException(
          "MBean descriptor cannot be null"));
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("MBean descriptor is: " + mbeanDescriptor);

    Object returnValue = null;

    String lastUpdateField = "lastReturnedTimeStamp";

    // Check if the method should be invoked given the cache settings
    int staleness = getStaleness(operationDescriptor, mbeanDescriptor, lastUpdateField);

    if (staleness == ALWAYS_STALE || staleness == STALE) {
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("Value is stale");

      // Find parameters classes
      Class[] parameters = null;
      try {
        parameters = Utils.loadClasses(Thread.currentThread().getContextClassLoader(), params);
      } catch (ClassNotFoundException x) {
        logger.error("Cannot find operation's parameter classes", x);
        throw new ReflectionException(x);
      }

      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("Invoking operation...");

      // Find target object
      Object target = resolveTargetObject(operationDescriptor);
      returnValue = invokeMethod(target, method, parameters, arguments);

      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("Returned value is: " + returnValue);

      if (returnValue != null) {
        Class parameter = returnValue.getClass();
        Class declared = loadClassWithContextClassLoader(operInfo.getReturnType());

        checkAssignability(parameter, declared);
      }

      // Cache the new value only if caching is needed
      if (staleness != ALWAYS_STALE) {
        operationDescriptor.setField("lastReturnedValue", returnValue);
        operationDescriptor.setField(lastUpdateField, Long.valueOf(System.currentTimeMillis()));
        if (logger.isEnabledFor(Logger.TRACE)) {
          logger.trace("Returned value has been cached");
        }

        // And now replace the descriptor with the updated clone
        info.setDescriptor(operationDescriptor, "operation");
      }

      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("invoke for operation " + method + " returns invoked value: " + returnValue);
    } else {
      // Return cached value
      returnValue = operationDescriptor.getFieldValue("lastReturnedValue");

      if (returnValue != null) {
        Class parameter = returnValue.getClass();
        Class declared = loadClassWithContextClassLoader(operInfo.getReturnType());

        checkAssignability(parameter, declared);
      }

      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("invoke for operation " + method + " returns cached value: " + returnValue);
    }

    // As an extension, persist this model mbean also after operation invocation, but using only
    // settings provided in the operation descriptor, without falling back to defaults set in
    // the MBean descriptor
    boolean persistNow = shouldPersistNow(operationDescriptor, null, lastUpdateField);
    int impact = operInfo.getImpact();
    if (persistNow && impact != MBeanOperationInfo.INFO) {
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("Persisting this ModelMBean...");
      try {
        store();
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("ModelMBean persisted successfully");
      } catch (Exception x) {
        logger.error(
            "Cannot store ModelMBean after operation invocation", x);
        if (x instanceof MBeanException)
          throw (MBeanException) x;
        else
          throw new MBeanException(x);
      }
    }

    return returnValue;
  }

  private Object resolveTargetObject(Descriptor descriptor) throws MBeanException {
    Logger logger = getLogger();
    Object target = descriptor.getFieldValue("targetObject");
    if (logger.isEnabledFor(Logger.TRACE))
      logger.trace("targetObject is: " + target);
    if (target == null) {
      target = getManagedResource();
    } else {
      String targetObjectType = (String) descriptor.getFieldValue("targetObjectType");
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("targetObjectType is: " + targetObjectType);
      if (targetObjectType == null) {
        // Not defined, assume object reference
        targetObjectType = OBJECT_RESOURCE_TYPE;
      }

      if (!isResourceTypeSupported(targetObjectType))
        throw new MBeanException(new InvalidTargetObjectTypeException(targetObjectType));
    }
    return target;
  }

  public void load() throws MBeanException, RuntimeOperationsException, InstanceNotFoundException {
    PersisterMBean persister = findPersister();
    if (persister != null) {
      ModelMBeanInfo info = (ModelMBeanInfo) persister.load();
      setModelMBeanInfo(info);
    }
  }

  public void store() throws MBeanException, RuntimeOperationsException, InstanceNotFoundException {
    PersisterMBean persister = findPersister();
    if (persister != null) {
      // Take a clone to avoid synchronization problems
      ModelMBeanInfo info = (ModelMBeanInfo) getMBeanInfo();
      persister.store(info);
    }
  }

  protected ClassLoaderRepository getClassLoaderRepository() {
    if (m_mbeanServer != null)
      return m_mbeanServer.getClassLoaderRepository();
    else
      return null;
  }


  private boolean shouldPersistNow(Descriptor attribute, Descriptor mbean, String lastUpdateField) {
    int persist = getPersistPolicy(attribute, mbean);
    if (persist == PERSIST_NO_MORE_OFTEN_THAN) {
      Long period = getFieldTimeValue(attribute, mbean, "persistPeriod");
      long now = System.currentTimeMillis();
      Long lastUpdate = (Long) attribute.getFieldValue(lastUpdateField);
      if (now - lastUpdate.longValue() < period.longValue())
        return false;
      else
        return true;
    } else if (persist == PERSIST_NEVER) {
      return false;
    } else if (persist == PERSIST_ON_TIMER) {
      return false;
    } else if (persist == PERSIST_ON_UPDATE) {
      return true;
    } else {
      throw new ImplementationException(
          "Invalid persist value");
    }
  }

  private int getPersistPolicy(Descriptor descriptor, Descriptor mbean) {
    Logger logger = getLogger();

    String persist = (String) descriptor.getFieldValue("persistPolicy");
    if (persist == null && mbean != null)
      persist = (String) mbean.getFieldValue("persistPolicy");
    if (persist == null) {
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("No persist policy defined, assuming Never");
      return PERSIST_NEVER;
    } else {
      if (persist.equals("Never")) {
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("Persist never");
        return PERSIST_NEVER;
      } else if (persist.equals("OnUpdate")) {
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("Persist on update");
        return PERSIST_ON_UPDATE;
      } else if (persist.equals("OnTimer")) {
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("Persist on update");
        return PERSIST_ON_TIMER;
      } else if (persist.equals("NoMoreOftenThan")) {
        if (logger.isEnabledFor(Logger.TRACE)) {
          Long period = getFieldTimeValue(descriptor, mbean, "persistPeriod");
          logger.trace("Persist no more often than " + period);
        }
        return PERSIST_NO_MORE_OFTEN_THAN;
      } else {
        // Garbage, assuming Never
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("Invalid persist policy, assuming persist never");
        return PERSIST_NEVER;
      }
    }
  }

  private int getStaleness(Descriptor attribute, Descriptor mbean, String lastUpdateField) {
    Logger logger = getLogger();

    Long currencyTimeLimit = getFieldTimeValue(attribute, mbean, "currencyTimeLimit");
    if (currencyTimeLimit == null) {
      // No time limit defined
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("No currencyTimeLimit defined, assuming always stale");
      return ALWAYS_STALE;
    } else {
      long ctl = currencyTimeLimit.longValue() * 1000;
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("currencyTimeLimit is (ms): " + ctl);

      if (ctl == 0) {
        // Never stale
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("Never stale");
        return NEVER_STALE;
      } else if (ctl < 0) // this should be == -1 but the other cases are in the air
      {
        // Always stale
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("Always stale");
        return ALWAYS_STALE;
      } else {
        Long timestamp = (Long) attribute.getFieldValue(lastUpdateField);
        long luts = 0;

        if (timestamp != null)
          luts = timestamp.longValue();
        if (logger.isEnabledFor(Logger.DEBUG))
          logger.debug(lastUpdateField + " is: " + luts);

        long now = System.currentTimeMillis();
        if (now < luts + ctl) {
          // Seems to be not stale, but has been set at least once ?
          if (timestamp == null) {
            // Return stale to call it the first time
            if (logger.isEnabledFor(Logger.TRACE))
              logger.trace("Stale since was never set");
            return STALE;
          } else {
            if (logger.isEnabledFor(Logger.TRACE))
              logger.trace("Not stale");
            return NOT_STALE;
          }
        } else {
          // Stale
          if (logger.isEnabledFor(Logger.TRACE))
            logger.trace("Stale");
          return STALE;
        }
      }
    }
  }

  private Long getFieldTimeValue(Descriptor descriptor, Descriptor mbean, String field) {
    Logger logger = getLogger();

    Object value = descriptor.getFieldValue(field);
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Descriptor's " + field + " field: " + value);

    if (value == null && mbean != null) {
      value = mbean.getFieldValue(field);
      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("MBean's " + field + " field: " + value);
      if (value == null)
        return null;
    }

    if (value instanceof Number)
      return Long.valueOf(((Number) value).longValue());

    if (value instanceof String) {
      try {
        long ctl = Long.parseLong((String) value);
        return Long.valueOf(ctl);
      } catch (NumberFormatException x) {
        return Long.valueOf(0);
      }
    }
    return Long.valueOf(0);
  }

  private Object invokeMethod(Object target, String methodName, Class[] params, Object[] args)
      throws MBeanException, ReflectionException {
    // First try on this instance, then on the target
    Object realTarget = null;
    Method method = null;
    try {
      realTarget = this;
      method = realTarget.getClass().getMethod(methodName, params);
    } catch (NoSuchMethodException x) {
      realTarget = target;
    }

    if (realTarget == null)
      throw new MBeanException(new ServiceNotFoundException(
          "Could not find target"));

    if (method == null) {
      try {
        method = realTarget.getClass().getMethod(methodName, params);
      } catch (NoSuchMethodException x) {
        throw new ReflectionException(x);
      }
    }

    try {
      Object value = method.invoke(realTarget, args);
      Logger logger = getLogger();
      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("Method invocation returned value: " + value);
      return value;
    } catch (IllegalAccessException x) {
      throw new ReflectionException(x);
    } catch (IllegalArgumentException x) {
      throw new MBeanException(x);
    } catch (InvocationTargetException x) {
      Throwable t = x.getTargetException();
      if (t instanceof Error)
        throw new MBeanException(new RuntimeErrorException((Error) t));
      else
        throw new MBeanException((Exception) t);
    }
  }

  private Logger getModelMBeanLogger(String notificationType) throws MBeanException {
    // Get a copy to avoid synchronization
    ModelMBeanInfo info = getModelMBeanInfo();

    // First look if there is a suitable notification descriptor, otherwise use MBean descriptor
    Descriptor descriptor = null;
    Logger modelMBeanLogger = null;
    if (notificationType != null) {
      descriptor = info.getDescriptor(notificationType, "notification");
      modelMBeanLogger = findLogger(descriptor);
    }

    if (modelMBeanLogger == null) {
      descriptor = info.getMBeanDescriptor();
      modelMBeanLogger = findLogger(descriptor);
      if (modelMBeanLogger != null)
        return modelMBeanLogger;
    }

    return null;
  }

  private Logger findLogger(Descriptor descriptor) {
    Logger logger = getLogger();

    if (descriptor == null) {
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("Can't find MBean logger, descriptor is null");
      return null;
    }

    String log = (String) descriptor.getFieldValue("log");
    String location = (String) descriptor.getFieldValue("logFile");

    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Log fields: log=" + log + ", file=" + location);

    if (log == null || !Boolean.valueOf(log).booleanValue()) {
      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("Logging is not supported by this ModelMBean");
      return null;
    }
    // Logger is supported, where log to ?
    if (location == null) {
      // As an extension, see if the field logMBean has been defined
      location = (String) descriptor.getFieldValue("logMBean");
      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("Log fields: mbean=" + location);

      if (location == null) {
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("Logging is not supported by this ModelMBean");
        return null;
      }

      // It seems that the user wants to delegate a registered mbean to log
      try {
        ObjectName objectName = new ObjectName(location);
        MBeanServer server = getMBeanServer();
        if (server == null)
          throw new MBeanException(new IllegalStateException(
              "MX4JModelMBean is not registered."));
        if (server.isRegistered(objectName)) {
          MBeanLogger l = new MBeanLogger(server, objectName);
          if (logger.isEnabledFor(Logger.DEBUG))
            logger.debug("ModelMBean log supported by delegating to this MBean: " + objectName);
          return l;
        }

        return null;
      } catch (MalformedObjectNameException x) {
        // Ah, was not a correct object name
        if (logger.isEnabledFor(Logger.DEBUG))
          logger.debug("Specified logMBean field does not contain a valid ObjectName: " + location);
        return null;
      } catch (MBeanException x) {
        if (logger.isEnabledFor(Logger.DEBUG))
          logger.debug("logMBean field does not specify an MBean that supports logging delegation",
              x);
        return null;
      }
    } else {
      // User decided to log to a file
      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("ModelMBean log supported on file system");
      return new FileLogger(location);
    }
  }

  private NotificationBroadcasterSupport getAttributeChangeBroadcaster() {
    return m_attributeChangeBroadcaster;
  }

  private MBeanServer getMBeanServer() {
    return m_mbeanServer;
  }

  private ModelMBeanInfo getModelMBeanInfo() {
    // No cloning performed
    return m_modelMBeanInfo;
  }

  private PersisterMBean findPersister() throws MBeanException, InstanceNotFoundException {
    Logger logger = getLogger();

    ModelMBeanInfo info = getModelMBeanInfo();
    if (info == null) {
      // Not yet initialized
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("Can't find persister, ModelMBeanInfo is null");
      return null;
    }
    Descriptor mbeanDescriptor = info.getMBeanDescriptor();
    if (mbeanDescriptor == null) {
      // This is normally should not happen if ModelMBeanInfoSupport is used
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("Can't find persister, MBean descriptor is null");
      return null;
    }

    String location = (String) mbeanDescriptor.getFieldValue("persistLocation");
    String name = (String) mbeanDescriptor.getFieldValue("persistName");
    String mbeanName = (String) mbeanDescriptor.getFieldValue("name");
    if (logger.isEnabledFor(Logger.DEBUG))
      logger.debug("Persistence fields: location=" + location + ", name=" + name);

    if (mbeanName == null && name == null) {
      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("Persistence is not supported by this ModelMBean");
      return null;
    }

    // Try to see if this mbean should delegate to another mbean
    if (name != null) {
      try {
        ObjectName objectName = new ObjectName(name.trim());
        // OK, a valid object name
        MBeanServer server = getMBeanServer();
        if (server == null)
          throw new MBeanException(new IllegalStateException(
              "MX4JModelMBean is not registered."));

        if (server.isRegistered(objectName)
            && server.isInstanceOf(objectName, PersisterMBean.class.getName())) {
          // OK, the given mbean is registered with this mbean server
          PersisterMBean persister = new MBeanPersister(server, objectName);
          if (logger.isEnabledFor(Logger.DEBUG))
            logger.debug("Persistence is delegated to this MBean: " + objectName);
          return persister;
        } else {
          throw new InstanceNotFoundException(objectName.toString());
        }
      } catch (MalformedObjectNameException ignored) {
        // It does not delegates to another mbean, use default
        if (logger.isEnabledFor(Logger.TRACE))
          logger.trace("Persistence is not delegated to another MBean");
      }

      // Default is serialization to file
      FilePersister persister = new FilePersister(location, name);
      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("Persistence is realized through file system in " + persister.getFileName());
      return persister;
    } else {
      // Only location given, use MBean name
      FilePersister persister = new FilePersister(location, mbeanName);
      if (logger.isEnabledFor(Logger.DEBUG))
        logger.debug("Persistence is realized through file system in " + persister.getFileName());
      return persister;
    }
  }

  private Class loadClassWithContextClassLoader(String name) {
    try {
      return Utils.loadClass(Thread.currentThread().getContextClassLoader(), name);
    } catch (ClassNotFoundException x) {
      Logger logger = getLogger();
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace("Cannot find attribute's declared return class", x);
      return null;
    }
  }

  private void checkAssignability(Class parameter, Class declared) throws MBeanException {
    Logger logger = getLogger();

    if (logger.isEnabledFor(Logger.DEBUG)) {
      logger.debug("The class of the parameter is: " + parameter);
      if (parameter != null)
        logger.debug("The classloder of the parameter's class is: " + parameter.getClassLoader());
      logger.debug("The class declared as type of the attribute is: " + declared);
      if (declared != null)
        logger.debug(
            "The classloader of the declared parameter's class is: " + declared.getClassLoader());
    }

    boolean assignable = false;

    if (declared == null || parameter == null)
      assignable = false;
    else if (declared == boolean.class && parameter == Boolean.class)
      assignable = true;
    else if (declared == byte.class && parameter == Byte.class)
      assignable = true;
    else if (declared == char.class && parameter == Character.class)
      assignable = true;
    else if (declared == short.class && parameter == Short.class)
      assignable = true;
    else if (declared == int.class && parameter == Integer.class)
      assignable = true;
    else if (declared == long.class && parameter == Long.class)
      assignable = true;
    else if (declared == float.class && parameter == Float.class)
      assignable = true;
    else if (declared == double.class && parameter == Double.class)
      assignable = true;
    else
      assignable = declared.isAssignableFrom(parameter);

    if (!assignable) {
      if (logger.isEnabledFor(Logger.TRACE))
        logger.trace(
            "Parameter value's class and attribute's declared return class are not assignable");
      throw new MBeanException(new InvalidAttributeValueException(
          "Returned type and declared type are not assignable."));
    }
  }
}
